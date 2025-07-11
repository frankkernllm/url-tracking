// auto-attribution-recovery.js - Automated recovery for new conversions
// Automatically finds and applies attributions for new unattributed conversions

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid API key' })
    };
  }

  // Redis helper function
  const redis = (path) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    const { hours_back = 24 } = JSON.parse(event.body || '{}');
    
    console.log(`🎯 AUTO-ATTRIBUTION: Processing unattributed conversions from last ${hours_back} hours`);
    
    // 1. Find unattributed conversions from recent timeframe
    const unattributedConversions = await findUnattributedConversions(redis, hours_back);
    console.log(`📊 Found ${unattributedConversions.length} unattributed conversions`);

    if (unattributedConversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'No unattributed conversions found',
          processed: 0,
          attributed: 0
        })
      };
    }

    let processed = 0;
    let attributed = 0;
    let errors = 0;
    const results = [];

    // 2. Process each unattributed conversion
    for (const conversion of unattributedConversions) {
      try {
        console.log(`\n🔄 Processing: ${conversion.email} (${conversion.redis_key})`);
        
        // 3. Query pageviews using enhanced query function
        const attributionResult = await queryPageviews(conversion);
        
        if (attributionResult.matches_found && attributionResult.matches_found.length > 0) {
          // 4. Get complete customer journey (optional - for enhanced reporting)
          const journeyResult = await getCustomerJourney(conversion, attributionResult);
          
          // 5. Update Redis directly with attribution data
          const updateResult = await updateConversionAttribution(redis, conversion, attributionResult, journeyResult);
          
          if (updateResult.success) {
            attributed++;
            results.push({
              email: conversion.email,
              status: 'attributed',
              method: attributionResult.best_match?.attribution_method || 'unknown',
              confidence: attributionResult.best_match?.confidence || 0,
              landing_page: attributionResult.best_match?.landing_page,
              redis_key: conversion.redis_key,
              journey_pageviews: journeyResult?.total_pageviews || 1,
              journey_duration_minutes: journeyResult?.total_duration_minutes || 0
            });
            console.log(`✅ Attribution applied for ${conversion.email}: ${attributionResult.best_match?.attribution_method} (${journeyResult?.total_pageviews || 1} pageviews)`);
          } else {
            errors++;
            results.push({
              email: conversion.email,
              status: 'update_failed',
              error: updateResult.error
            });
          }
        } else {
          results.push({
            email: conversion.email,
            status: 'no_attribution',
            message: 'No matching pageviews found'
          });
          console.log(`❌ No attribution found for ${conversion.email}`);
        }
        
        processed++;
      } catch (error) {
        errors++;
        console.error(`❌ Error processing ${conversion.email}:`, error);
        results.push({
          email: conversion.email,
          status: 'error',
          error: error.message
        });
      }
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        summary: {
          total_found: unattributedConversions.length,
          processed,
          attributed,
          errors,
          attribution_rate: Math.round((attributed / processed) * 100) || 0
        },
        results,
        timestamp: new Date().toISOString()
      })
    };

  } catch (error) {
    console.error('Auto-attribution error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: error.message })
    };
  }
};

// Find unattributed conversions from recent timeframe
async function findUnattributedConversions(redis, hoursBack) {
  const cutoffTime = Date.now() - (hoursBack * 60 * 60 * 1000);
  const conversions = [];
  
  // Scan for conversion keys
  let cursor = '0';
  do {
    const scanResult = await redis(`scan/${cursor}/match/conversion:*/count/1000`);
    if (scanResult.result) {
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      
      // Get conversion data for each key
      for (const key of keys) {
        try {
          const conversionData = await redis(`get/${key}`);
          if (conversionData.result) {
            const conversion = JSON.parse(decodeURIComponent(conversionData.result));
            
            // Check if within timeframe and unattributed
            const conversionTime = new Date(conversion.timestamp).getTime();
            if (conversionTime >= cutoffTime && !isAttributed(conversion)) {
              conversions.push({
                ...conversion,
                redis_key: key
              });
            }
          }
        } catch (error) {
          console.warn(`⚠️ Failed to parse conversion ${key}:`, error);
        }
      }
    }
  } while (cursor !== '0' && conversions.length < 1000);
  
  return conversions;
}

// Check if conversion already has attribution
function isAttributed(conversion) {
  return !!(
    conversion.attribution_found === true ||
    conversion.landing_page ||
    conversion.utm_source ||
    conversion.attribution_method
  );
}

// Query pageviews using enhanced multi-signal query-pageviews function
async function queryPageviews(conversion) {
  const queryData = {
    conversion_timestamp: conversion.timestamp,
    ips_to_check: [
      conversion.primary_ip || conversion.PIP,
      conversion.conversion_ip || conversion.CIP,
      conversion.ip_address || conversion.IP
    ].filter(Boolean),
    window_hours: 24,
    session_id: conversion.session_id || conversion.SSID,
    device_signature: conversion.device_signature || conversion.dsig,
    screen_value: conversion.screen_value || conversion.SVV,
    gpu_signature: conversion.gpu_signature || conversion.gsig
  };
  
  console.log(`   🔍 Query signals: IPs=${queryData.ips_to_check.length}, Session=${!!queryData.session_id}, Device=${!!queryData.device_signature}`);
  
  const response = await fetch(`https://trackingojoy.netlify.app/.netlify/functions/query-pageviews-enhanced`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': process.env.OJOY_API_KEY
    },
    body: JSON.stringify(queryData)
  });
  
  if (!response.ok) {
    throw new Error(`Query pageviews failed: ${response.status}`);
  }
  
  const result = await response.json();
  
  // Find best match (highest confidence/score)
  if (result.matches_found && result.matches_found.length > 0) {
    result.best_match = result.matches_found.reduce((best, current) => {
      const currentScore = current.confidence || current.attribution_score || 0;
      const bestScore = best.confidence || best.attribution_score || 0;
      return currentScore > bestScore ? current : best;
    });
  }
  
  return result;
}

// Get complete customer journey (enhanced reporting)
async function getCustomerJourney(conversion, attributionResult) {
  try {
    const response = await fetch(`https://trackingojoy.netlify.app/.netlify/functions/customer-journey`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': process.env.OJOY_API_KEY
      },
      body: JSON.stringify({
        email: conversion.email,
        conversion_timestamp: conversion.timestamp,
        ips_to_check: [
          conversion.primary_ip || conversion.PIP,
          conversion.conversion_ip || conversion.CIP,
          conversion.ip_address || conversion.IP
        ].filter(Boolean),
        session_id: conversion.session_id || conversion.SSID,
        device_signature: conversion.device_signature || conversion.dsig,
        screen_value: conversion.screen_value || conversion.SVV,
        gpu_signature: conversion.gpu_signature || conversion.gsig,
        journey_window_days: 7
      })
    });
    
    if (!response.ok) {
      console.warn(`⚠️ Journey mapping failed for ${conversion.email}: ${response.status}`);
      return null;
    }
    
    const journeyData = await response.json();
    return journeyData.summary; // Return just the summary for efficiency
    
  } catch (error) {
    console.warn(`⚠️ Journey mapping error for ${conversion.email}:`, error.message);
    return null;
  }
}

// Update conversion in Redis with attribution data and journey info
async function updateConversionAttribution(redis, conversion, attributionResult, journeyResult = null) {
  try {
    const bestMatch = attributionResult.best_match;
    
    // Create updated conversion data
    const updatedConversion = {
      ...conversion,
      // Attribution results
      attribution_found: true,
      attribution_method: bestMatch.attribution_method,
      attribution_score: bestMatch.confidence || bestMatch.attribution_score || 0,
      attribution_timestamp: new Date().toISOString(),
      
      // Landing page and UTM data from attribution
      landing_page: bestMatch.landing_page,
      utm_source: bestMatch.utm_source || bestMatch.source,
      utm_campaign: bestMatch.utm_campaign || bestMatch.campaign,
      utm_medium: bestMatch.utm_medium,
      utm_content: bestMatch.utm_content,
      utm_term: bestMatch.utm_term,
      
      // Original pageview metadata
      pageview_timestamp: bestMatch.timestamp,
      pageview_session_id: bestMatch.session_id,
      
      // Customer journey data (if available)
      journey_total_pageviews: journeyResult?.total_pageviews || 1,
      journey_unique_pages: journeyResult?.unique_pages?.length || 1,
      journey_duration_minutes: journeyResult?.total_duration_minutes || 0,
      journey_first_touch: journeyResult?.first_touch?.timestamp || bestMatch.timestamp,
      journey_last_touch: journeyResult?.last_touch?.timestamp || bestMatch.timestamp,
      
      // Processing metadata
      auto_attributed: true,
      processing_version: 'auto-recovery-v2-with-journey'
    };
    
    // Update Redis
    const updateResult = await redis(`set/${conversion.redis_key}/${encodeURIComponent(JSON.stringify(updatedConversion))}`);
    
    if (updateResult.result === 'OK') {
      console.log(`✅ Updated Redis for ${conversion.email}: ${bestMatch.attribution_method}`);
      return { success: true };
    } else {
      console.error(`❌ Redis update failed for ${conversion.email}:`, updateResult);
      return { success: false, error: 'Redis update failed' };
    }
    
  } catch (error) {
    console.error(`❌ Update error for ${conversion.email}:`, error);
    return { success: false, error: error.message };
  }
}
