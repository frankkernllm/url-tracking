// Staged Recovery System - Safe Attribution Recovery with Review Process
// Path: netlify/functions/staged-recovery.js

exports.handler = async (event, context) => {
  // Environment variable validation
  const requiredEnvVars = [
    'UPSTASH_REDIS_REST_URL',
    'UPSTASH_REDIS_REST_TOKEN'
  ];
  
  const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
  if (missingEnvVars.length > 0) {
    console.log('Missing environment variables:', missingEnvVars);
    return {
      statusCode: 500,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
      },
      body: JSON.stringify({ 
        error: 'Configuration error',
        missing: missingEnvVars
      })
    };
  }

  // Redis helper using Upstash REST API (same as track.js)
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  const redis = async (command, timeoutMs = 5000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
      controller.abort();
      console.log(`⏰ Redis timeout after ${timeoutMs}ms for command: ${command.split('/')[0]}`);
    }, timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { 
          Authorization: `Bearer ${redisToken}`,
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        const errorText = await response.text();
        console.log(`❌ Redis HTTP error ${response.status}: ${errorText}`);
        throw new Error(`Redis HTTP error: ${response.status} ${errorText}`);
      }
      
      const result = await response.json();
      console.log(`✅ Redis command success: ${command.split('/')[0]} -> ${JSON.stringify(result).substring(0, 100)}`);
      
      return result;
      
    } catch (error) {
      clearTimeout(timeoutId);
      
      if (error.name === 'AbortError') {
        console.log(`⏰ Redis command timed out: ${command.split('/')[0]}`);
        throw new Error(`Redis timeout after ${timeoutMs}ms`);
      }
      
      console.log(`❌ Redis command failed: ${command.split('/')[0]} - ${error.message}`);
      throw error;
    }
  };
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  // Handle different operations
  const path = event.path.split('/').pop();
  
  if (event.httpMethod === 'POST' && path === 'stage-recovery') {
    return await stageRecovery(event, headers);
  } else if (event.httpMethod === 'GET' && path === 'review-staged') {
    return await reviewStagedRecoveries(event, headers);
  } else if (event.httpMethod === 'POST' && path === 'apply-recovery') {
    return await applyStagedRecovery(event, headers);
  } else if (event.httpMethod === 'POST' && path === 'clear-staging') {
    return await clearStagingArea(event, headers);
  }

  return {
    statusCode: 404,
    headers,
    body: JSON.stringify({ error: 'Endpoint not found' })
  };
};

// Stage a recovery without updating live data
async function stageRecovery(event, headers) {
  try {
    const data = JSON.parse(event.body);
    
    console.log('🎭 Staging recovery for:', {
      email: data.email,
      order_id: data.order_id
    });

    // Extract IPs using correct mapping
    const pageviewIP = data.ip;
    const conversionIP = data.checkoutview?.pageviewcheckout?.pageview?.ip || data.conversion_ip;
    
    console.log('📍 IP Analysis:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP
    });

    // Attempt to find attribution using pageview IP
    const attributionResult = await findAttributionByPageviewIP(pageviewIP, data.timestamp, redis);
    
    // Find the current conversion record (read-only)
    const currentConversion = await findCurrentConversion(data.email, data.timestamp, redis);
    
    if (attributionResult && currentConversion) {
      // Stage the recovery (don't update live data yet)
      const stagedRecovery = {
        // Recovery metadata
        recovery_id: `recovery_${Date.now()}_${data.email.replace('@', '_at_')}`,
        timestamp: new Date().toISOString(),
        status: 'staged',
        
        // Original data
        original_conversion: currentConversion,
        
        // What would be recovered
        recovered_attribution: attributionResult,
        
        // Proposed changes
        proposed_changes: {
          landing_page: {
            current: currentConversion.landing_page,
            proposed: attributionResult.landing_page
          },
          source: {
            current: currentConversion.source,
            proposed: attributionResult.source
          },
          utm_campaign: {
            current: currentConversion.utm_campaign,
            proposed: attributionResult.utm_campaign
          },
          utm_source: {
            current: currentConversion.utm_source,
            proposed: attributionResult.utm_source
          },
          utm_medium: {
            current: currentConversion.utm_medium,
            proposed: attributionResult.utm_medium
          }
        },
        
        // Technical details
        recovery_method: 'pageview_ip_match',
        pageview_ip: pageviewIP,
        conversion_ip: conversionIP,
        attribution_confidence: attributionResult.confidence || 'medium',
        
        // Validation flags
        needs_review: shouldFlagForReview(currentConversion, attributionResult),
        risk_level: assessRiskLevel(currentConversion, attributionResult)
      };

      // Store in staging area
      const stagingKey = `recovery_staging:${stagedRecovery.recovery_id}`;
      await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);
      
      // Add to staging index
      await addToStagingIndex(stagedRecovery.recovery_id, data.email, redis);

      console.log('✅ Recovery staged successfully');

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: true,
          recovery_id: stagedRecovery.recovery_id,
          attribution_found: true,
          needs_review: stagedRecovery.needs_review,
          risk_level: stagedRecovery.risk_level,
          proposed_changes: stagedRecovery.proposed_changes,
          message: 'Recovery staged successfully - review before applying'
        })
      };

    } else {
      console.log('❌ No attribution found or conversion not found');
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: false,
          attribution_found: !!attributionResult,
          conversion_found: !!currentConversion,
          message: 'No recovery possible - no attribution found or conversion not found'
        })
      };
    }

  } catch (error) {
    console.error('❌ Staging error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Staging failed', message: error.message })
    };
  }
}

// Review all staged recoveries
async function reviewStagedRecoveries(event, headers, redis) {
  try {
    console.log('📋 Reviewing staged recoveries...');
    
    // Get all staging keys
    const stagingKeys = await redis('keys/recovery_staging:*');
    
    if (!stagingKeys?.result?.length) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged_recoveries: [],
          total: 0,
          message: 'No staged recoveries found'
        })
      };
    }

    const stagedRecoveries = [];
    const summary = {
      total: 0,
      needs_review: 0,
      low_risk: 0,
      medium_risk: 0,
      high_risk: 0,
      by_source: {}
    };

    // Load each staged recovery
    for (const key of stagingKeys.result) {
      try {
        const data = await redis(`get/${key}`);
        if (data?.result) {
          const recovery = JSON.parse(data.result);
          stagedRecoveries.push(recovery);
          
          // Update summary
          summary.total++;
          if (recovery.needs_review) summary.needs_review++;
          summary[recovery.risk_level + '_risk']++;
          
          const source = recovery.recovered_attribution?.source || 'unknown';
          summary.by_source[source] = (summary.by_source[source] || 0) + 1;
        }
      } catch (parseError) {
        console.log(`⚠️ Error parsing ${key}:`, parseError.message);
      }
    }

    // Sort by risk level and timestamp
    stagedRecoveries.sort((a, b) => {
      const riskOrder = { high: 3, medium: 2, low: 1 };
      const aRisk = riskOrder[a.risk_level] || 0;
      const bRisk = riskOrder[b.risk_level] || 0;
      
      if (aRisk !== bRisk) return bRisk - aRisk; // High risk first
      return new Date(b.timestamp) - new Date(a.timestamp); // Then by newest
    });

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        staged_recoveries: stagedRecoveries,
        summary: summary,
        review_url: '/.netlify/functions/staged-recovery/review-staged'
      })
    };

  } catch (error) {
    console.error('❌ Review error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Review failed', message: error.message })
    };
  }
}

// Apply a specific staged recovery to live data
async function applyStagedRecovery(event, headers, redis) {
  try {
    const { recovery_id, approved_by } = JSON.parse(event.body);
    
    console.log(`🚀 Applying staged recovery: ${recovery_id}`);
    
    // Load staged recovery
    const stagingKey = `recovery_staging:${recovery_id}`;
    const stagingData = await redis(`get/${stagingKey}`);
    
    if (!stagingData?.result) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ error: 'Staged recovery not found' })
      };
    }

    const stagedRecovery = JSON.parse(stagingData.result);
    
    // Apply the recovery to live data
    const conversionKey = stagedRecovery.original_conversion.key;
    const updatedConversion = {
      ...stagedRecovery.original_conversion,
      
      // Apply recovered attribution
      landing_page: stagedRecovery.recovered_attribution.landing_page,
      source: stagedRecovery.recovered_attribution.source,
      utm_campaign: stagedRecovery.recovered_attribution.utm_campaign,
      utm_source: stagedRecovery.recovered_attribution.utm_source,
      utm_medium: stagedRecovery.recovered_attribution.utm_medium,
      utm_term: stagedRecovery.recovered_attribution.utm_term,
      utm_content: stagedRecovery.recovered_attribution.utm_content,
      
      // Recovery metadata
      recovery_applied: true,
      recovery_timestamp: new Date().toISOString(),
      recovery_method: 'staged_pageview_ip_match',
      recovery_id: recovery_id,
      approved_by: approved_by || 'system',
      pageview_ip_used: stagedRecovery.pageview_ip
    };

    // Update live conversion data
    await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(updatedConversion))}`);
    
    // Mark staged recovery as applied
    stagedRecovery.status = 'applied';
    stagedRecovery.applied_timestamp = new Date().toISOString();
    stagedRecovery.approved_by = approved_by;
    await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);

    console.log('✅ Recovery applied to live data');

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        recovery_id: recovery_id,
        applied: true,
        updated_fields: Object.keys(stagedRecovery.proposed_changes),
        message: 'Recovery successfully applied to live conversion data'
      })
    };

  } catch (error) {
    console.error('❌ Apply error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Apply failed', message: error.message })
    };
  }
}

// Clear staging area (for cleanup)
async function clearStagingArea(event, headers, redis) {
  try {
    const { confirm, keep_applied } = JSON.parse(event.body || '{}');
    
    if (confirm !== 'yes') {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: 'Must confirm with "confirm": "yes"' })
      };
    }

    const stagingKeys = await redis('keys/recovery_staging:*');
    let deleted = 0;
    let kept = 0;

    if (stagingKeys?.result?.length) {
      for (const key of stagingKeys.result) {
        if (keep_applied) {
          // Check if this recovery was applied
          const data = await redis(`get/${key}`);
          if (data?.result) {
            const recovery = JSON.parse(data.result);
            if (recovery.status === 'applied') {
              kept++;
              continue;
            }
          }
        }
        
        await redis(`del/${key}`);
        deleted++;
      }
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        deleted: deleted,
        kept: kept,
        message: `Staging area cleared - ${deleted} removed, ${kept} kept`
      })
    };

  } catch (error) {
    console.error('❌ Clear error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Clear failed', message: error.message })
    };
  }
}

// Helper functions

async function findAttributionByPageviewIP(pageviewIP, originalTimestamp, redis) {
  try {
    if (!pageviewIP) return null;
    
    console.log('🔍 Searching for attribution with pageview IP:', pageviewIP);
    
    // Method 1: Try IP lookup keys
    const ipKey = `attribution_ip_${encodeIPForKey(pageviewIP)}`;
    let lookupResult = await redis(`get/${ipKey}`);
    
    if (lookupResult?.result) {
      const mainKey = lookupResult.result;
      const attributionData = await redis(`get/${mainKey}`);
      if (attributionData?.result) {
        const attribution = JSON.parse(attributionData.result);
        attribution.confidence = 'high';
        attribution.method = 'ip_lookup_key';
        return attribution;
      }
    }

    // Method 2: Scan attribution records
    const attributionKeys = await redis('keys/attribution_*');
    
    if (!attributionKeys?.result) return null;

    for (const key of attributionKeys.result) {
      if (key.startsWith('attribution_ip_') || key.startsWith('attribution_session_')) {
        continue;
      }
      
      const data = await redis(`get/${key}`);
      if (data?.result) {
        const attribution = JSON.parse(data.result);
        
        if (attribution.ip_address === pageviewIP) {
          const timeDiff = Math.abs(new Date(originalTimestamp) - new Date(attribution.timestamp));
          const oneDayMs = 24 * 60 * 60 * 1000;
          
          if (timeDiff < oneDayMs) {
            attribution.confidence = 'medium';
            attribution.method = 'ip_scan_match';
            return attribution;
          }
        }
      }
    }
    
    return null;
    
  } catch (error) {
    console.error('❌ Error finding attribution:', error);
    return null;
  }
}

async function findCurrentConversion(email, timestamp, redis) {
  try {
    const conversionKeys = await redis('keys/conversions:*');
    
    if (!conversionKeys?.result) return null;

    for (const key of conversionKeys.result) {
      const conversionData = await redis(`get/${key}`);
      if (conversionData?.result) {
        const conversion = JSON.parse(conversionData.result);
        
        if (conversion.email === email && conversion.timestamp === timestamp) {
          conversion.key = key; // Store the key for later updates
          return conversion;
        }
      }
    }
    
    return null;
  } catch (error) {
    console.error('❌ Error finding conversion:', error);
    return null;
  }
}

function shouldFlagForReview(currentConversion, attributionResult) {
  // Flag for review if:
  
  // 1. Significant change in source
  const currentSource = currentConversion.source?.toLowerCase() || 'direct';
  const newSource = attributionResult.source?.toLowerCase() || 'unknown';
  
  if (currentSource === 'direct' && newSource !== 'direct') {
    return true; // Direct -> Attributed source
  }
  
  // 2. Landing page seems suspicious
  if (attributionResult.landing_page?.includes('test') || 
      attributionResult.landing_page?.includes('staging')) {
    return true;
  }
  
  // 3. Attribution is older than 48 hours
  const timeDiff = Math.abs(new Date(currentConversion.timestamp) - new Date(attributionResult.timestamp));
  if (timeDiff > 48 * 60 * 60 * 1000) {
    return true;
  }
  
  return false;
}

function assessRiskLevel(currentConversion, attributionResult) {
  let riskScore = 0;
  
  // Low risk indicators
  if (attributionResult.confidence === 'high') riskScore -= 1;
  if (attributionResult.method === 'ip_lookup_key') riskScore -= 1;
  
  // Medium risk indicators
  if (!currentConversion.landing_page && attributionResult.landing_page) riskScore += 1;
  
  // High risk indicators
  if (currentConversion.source !== 'direct' && 
      attributionResult.source !== currentConversion.source) riskScore += 2;
  
  if (attributionResult.landing_page?.includes('test')) riskScore += 3;
  
  if (riskScore <= 0) return 'low';
  if (riskScore <= 2) return 'medium';
  return 'high';
}

async function addToStagingIndex(recoveryId, email, redis) {
  try {
    const indexKey = 'recovery_staging_index';
    const indexEntry = { recovery_id: recoveryId, email: email, timestamp: new Date().toISOString() };
    
    // This is a simple approach - in production you might want a more sophisticated index
    await redis(`set/${indexKey}:${recoveryId}/${encodeURIComponent(JSON.stringify(indexEntry))}`);
  } catch (error) {
    console.log('⚠️ Failed to update staging index:', error.message);
  }
}

function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_');
}
