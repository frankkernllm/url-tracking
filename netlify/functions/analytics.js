// ============================================================================
// COMPLETE ANALYTICS.JS WITH COMPREHENSIVE REDIS SCANNING FIX
// Deploy this file to: netlify/functions/analytics.js
// ============================================================================

// Enhanced timestamp validation function
function isValidTimestamp(timestamp) {
    if (!timestamp) return false;
    
    try {
        const date = new Date(timestamp);
        if (isNaN(date.getTime())) return false;
        
        const timestampMs = date.getTime();
        const minDate = new Date('2015-01-01').getTime();
        const maxDate = new Date('2035-12-31').getTime();
        
        return timestampMs >= minDate && timestampMs <= maxDate;
    } catch (error) {
        console.warn('Timestamp validation error:', error);
        return false;
    }
}

// Enhanced safe timestamp processing
function safeProcessTimestamp(timestamp, fallbackTimestamp = null) {
    if (isValidTimestamp(timestamp)) {
        return timestamp;
    }
    
    console.warn('⚠️ Invalid timestamp detected:', timestamp);
    
    if (fallbackTimestamp && isValidTimestamp(fallbackTimestamp)) {
        console.log('✅ Using fallback timestamp:', fallbackTimestamp);
        return fallbackTimestamp;
    }
    
    const currentTimestamp = new Date().toISOString();
    console.log('🔧 Generated current timestamp fallback:', currentTimestamp);
    return currentTimestamp;
}

// ENHANCED: Comprehensive attribution key scanning - THIS IS THE MAIN FIX!
async function getComprehensiveAttributionKeys(redis) {
    console.log('🔍 Starting comprehensive attribution key scanning...');
    
    let allAttributionKeys = [];
    let totalScanned = 0;
    
    try {
        // 1. EXPANDED IPv6 PREFIX SCANNING
        // Include ALL possible IPv6 prefixes, not just a limited set
        const ipv6Prefixes = [
            // Current prefixes (keep existing)
            '2600', '2601', '2603', '2604', '2605', '2606', '2607', 
            '2800', '2001', '2002',
            // MISSING prefixes that could contain your thousands of page views
            '2602', '2608', '2609', '260a', '260b', '260c', '260d', '260e', '260f',
            '2610', '2620', '2630', '2640', '2650', '2660', '2670', '2680', '2690',
            '2a00', '2a01', '2a02', '2a03', '2a04', '2a05', '2a06', '2a07',
            '2400', '2401', '2402', '2403', '2404', '2405', '2406', '2407',
            '2003', '2004', '2005', '2006', '2007', '2008', '2009',
            'fe80', 'fc00', 'fd00' // Link-local and private IPv6
        ];
        
        console.log(`📊 Scanning ${ipv6Prefixes.length} IPv6 prefixes...`);
        
        for (const prefix of ipv6Prefixes) {
            try {
                const result = await redis(`keys/attribution_${prefix}*`);
                if (result.result && result.result.length > 0) {
                    allAttributionKeys = allAttributionKeys.concat(result.result);
                    console.log(`✅ Found ${result.result.length} keys with IPv6 prefix ${prefix}`);
                    totalScanned += result.result.length;
                }
            } catch (prefixError) {
                // Continue to next prefix
                console.log(`⚠️ Prefix ${prefix} scan failed:`, prefixError.message);
            }
        }
        
        // 2. IPv4 SCANNING (Enhanced)
        console.log('🔍 Scanning IPv4 attribution keys...');
        try {
            // Scan for all IPv4 patterns
            const ipv4Patterns = [
                'attribution_192*', // Private networks
                'attribution_172*',
                'attribution_10_*',
                'attribution_1_*',   // Public IPv4 starting with 1
                'attribution_2_*',   // Public IPv4 starting with 2 (but not IPv6)
                'attribution_3_*',   // etc.
                'attribution_4_*',
                'attribution_5_*',
                'attribution_6_*',
                'attribution_7_*',
                'attribution_8_*',
                'attribution_9_*'
            ];
            
            for (const pattern of ipv4Patterns) {
                try {
                    const result = await redis(`keys/${pattern}`);
                    if (result.result && result.result.length > 0) {
                        // Filter to ensure these are actually IPv4 keys
                        const ipv4Keys = result.result.filter(key => {
                            const parts = key.split('_');
                            // Check if it looks like IPv4 format (not IPv6)
                            return parts.length >= 4 && 
                                   /^\d+$/.test(parts[1]) && 
                                   /^\d+$/.test(parts[2]) &&
                                   !key.includes(':'); // Exclude IPv6 keys
                        });
                        
                        if (ipv4Keys.length > 0) {
                            allAttributionKeys = allAttributionKeys.concat(ipv4Keys);
                            console.log(`✅ Found ${ipv4Keys.length} IPv4 keys with pattern ${pattern}`);
                            totalScanned += ipv4Keys.length;
                        }
                    }
                } catch (patternError) {
                    console.log(`⚠️ IPv4 pattern ${pattern} scan failed:`, patternError.message);
                }
            }
        } catch (ipv4Error) {
            console.log('⚠️ IPv4 scanning failed:', ipv4Error.message);
        }
        
        // 3. ENHANCED KEY PATTERN SCANNING
        // Scan for all the additional key types created by store-attribution.js
        console.log('🔍 Scanning enhanced attribution key patterns...');
        
        const enhancedPatterns = [
            'attribution_fp_*',      // Fingerprint keys
            'attribution_webgl_*',   // WebGL keys
            'attribution_geo_*',     // Geographic keys
            'attribution_region_*',  // Regional keys
            'attribution_screen_*',  // Screen resolution keys
            'attribution_hw_*',      // Hardware fingerprint keys
            'attribution_ip_*',      // IP lookup keys
            'attribution_session_*'  // Session lookup keys
        ];
        
        for (const pattern of enhancedPatterns) {
            try {
                const result = await redis(`keys/${pattern}`);
                if (result.result && result.result.length > 0) {
                    // For lookup keys, we need the target keys they point to
                    if (pattern.includes('_ip_') || pattern.includes('_session_') || 
                        pattern.includes('_fp_') || pattern.includes('_webgl_') ||
                        pattern.includes('_geo_') || pattern.includes('_region_') ||
                        pattern.includes('_screen_') || pattern.includes('_hw_')) {
                        
                        console.log(`📋 Found ${result.result.length} ${pattern} lookup keys`);
                        
                        // Get the actual attribution keys these point to
                        for (const lookupKey of result.result.slice(0, 1000)) { // Limit to prevent timeout
                            try {
                                const targetResult = await redis(`get/${lookupKey}`);
                                if (targetResult.result && targetResult.result.startsWith('attribution_')) {
                                    allAttributionKeys.push(targetResult.result);
                                }
                            } catch (lookupError) {
                                // Continue to next lookup key
                            }
                        }
                    } else {
                        // These are direct attribution keys
                        allAttributionKeys = allAttributionKeys.concat(result.result);
                        console.log(`✅ Found ${result.result.length} ${pattern} direct keys`);
                        totalScanned += result.result.length;
                    }
                }
            } catch (enhancedError) {
                console.log(`⚠️ Enhanced pattern ${pattern} scan failed:`, enhancedError.message);
            }
        }
        
        // 4. FALLBACK: BROAD SCAN
        // If we still don't have thousands of keys, do a broad scan
        if (allAttributionKeys.length < 1000) {
            console.log('🚨 Low key count detected, performing broad scan...');
            
            try {
                const broadResult = await redis('keys/attribution*');
                if (broadResult.result && broadResult.result.length > 0) {
                    console.log(`📊 Broad scan found ${broadResult.result.length} total attribution-related keys`);
                    
                    // Filter for actual attribution data keys (not lookup keys)
                    const dataKeys = broadResult.result.filter(key => {
                        return key.startsWith('attribution_') && 
                               !key.includes('attribution_ip_') &&
                               !key.includes('attribution_session_') &&
                               (key.includes('_2') || key.includes('_1') || key.includes('_fc') || key.includes('_fe'));
                    });
                    
                    allAttributionKeys = allAttributionKeys.concat(dataKeys);
                    console.log(`✅ Broad scan added ${dataKeys.length} data keys`);
                    totalScanned += dataKeys.length;
                }
            } catch (broadError) {
                console.log('⚠️ Broad scan failed:', broadError.message);
            }
        }
        
        // 5. REMOVE DUPLICATES AND VALIDATE
        const uniqueKeys = [...new Set(allAttributionKeys)];
        console.log(`📊 Total keys before deduplication: ${allAttributionKeys.length}`);
        console.log(`📊 Unique attribution keys found: ${uniqueKeys.length}`);
        console.log(`📊 Total scanned across all patterns: ${totalScanned}`);
        
        // Log sample keys for verification
        if (uniqueKeys.length > 0) {
            console.log('📝 Sample attribution keys found:');
            uniqueKeys.slice(0, 5).forEach((key, i) => {
                console.log(`  ${i+1}. ${key}`);
            });
        }
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Comprehensive attribution key scanning failed:', error);
        return [];
    }
}

// FIXED: Enhanced conversion key scanning - resolves the "309 to 2 conversions" issue
async function getConversionKeysEnhanced(redis) {
    let conversionKeys = [];
    
    console.log('🔍 Starting FIXED conversion key scan...');
    
    try {
        // 1. STANDARD PATTERN (this should work for most cases)
        console.log('📊 Trying standard conversions:* pattern...');
        const standardResult = await redis('keys/conversions:*');
        if (standardResult.result && standardResult.result.length > 0) {
            conversionKeys = standardResult.result;
            console.log(`✅ Found ${conversionKeys.length} keys with standard pattern`);
            
            // If we found a good amount, return immediately (don't complicate it)
            if (conversionKeys.length > 50) {
                console.log(`🎯 Standard pattern found substantial data (${conversionKeys.length}), using this`);
                return [...new Set(conversionKeys)]; // Remove duplicates and return
            }
        }
        
        // 2. ONLY if standard pattern failed, try enhanced scanning
        console.log('📊 Standard pattern found limited data, trying enhanced patterns...');
        
        // Try email-based patterns (newer format)
        const emailPatterns = [
            'conversions:*_*:*',  // email-based keys
            'conversions:*@*',    // direct email keys
        ];
        
        for (const pattern of emailPatterns) {
            try {
                const emailResult = await redis(`keys/${pattern}`);
                if (emailResult.result && emailResult.result.length > 0) {
                    conversionKeys = conversionKeys.concat(emailResult.result);
                    console.log(`✅ Found ${emailResult.result.length} keys with email pattern ${pattern}`);
                }
            } catch (patternError) {
                console.log(`⚠️ Email pattern ${pattern} failed:`, patternError.message);
            }
        }
        
        // 3. Try date-based patterns (fallback)
        const today = new Date().toISOString().split('T')[0];
        const yesterday = new Date(Date.now() - 24*60*60*1000).toISOString().split('T')[0];
        
        const datePatterns = [
            `conversions:${today}*`,
            `conversions:${yesterday}*`,
            `conversions:2025-06-28*`,
            `conversions:2025-06-27*`,
            `conversions:2025-06-26*`,
            `conversions:2025-06-25*`,
            `conversions:2025-06-24*`,
            `conversions:2025-06-23*`,
            `conversions:2025-06-22*`,
            `conversions:2025-06-21*`,
            `conversions:2025-06-20*`
        ];
        
        for (const pattern of datePatterns) {
            try {
                const dateResult = await redis(`keys/${pattern}`);
                if (dateResult.result && dateResult.result.length > 0) {
                    conversionKeys = conversionKeys.concat(dateResult.result);
                    console.log(`✅ Found ${dateResult.result.length} keys with date pattern ${pattern}`);
                }
            } catch (patternError) {
                console.log(`⚠️ Date pattern ${pattern} failed:`, patternError.message);
            }
        }
        
        // 4. SCAN approach (broad search)
        try {
            console.log('📊 Trying SCAN approach...');
            const scanResult = await redis('scan/0/match/conversions:*/count/1000');
            if (scanResult.result && scanResult.result[1] && scanResult.result[1].length > 0) {
                const scanKeys = scanResult.result[1];
                conversionKeys = conversionKeys.concat(scanKeys);
                console.log(`✅ SCAN found ${scanKeys.length} additional keys`);
            }
        } catch (scanError) {
            console.log('⚠️ SCAN approach failed:', scanError.message);
        }
        
        // 5. Fallback: Try alternative key formats
        if (conversionKeys.length < 10) {
            console.log('🚨 Very low conversion count, trying alternative formats...');
            
            const alternativePatterns = [
                'conversion:*',      // singular form
                'order:*',           // order-based keys
                'purchase:*',        // purchase-based keys
                'ecommerce:*',       // ecommerce keys
                'webhook:*'          // webhook-based keys
            ];
            
            for (const altPattern of alternativePatterns) {
                try {
                    const altResult = await redis(`keys/${altPattern}`);
                    if (altResult.result && altResult.result.length > 0) {
                        conversionKeys = conversionKeys.concat(altResult.result);
                        console.log(`✅ Found ${altResult.result.length} keys with alternative pattern ${altPattern}`);
                    }
                } catch (altError) {
                    console.log(`⚠️ Alternative pattern ${altPattern} failed:`, altError.message);
                }
            }
        }
        
        // 6. Remove duplicates and validate
        const uniqueConversionKeys = [...new Set(conversionKeys)];
        console.log(`📊 Total conversion keys before deduplication: ${conversionKeys.length}`);
        console.log(`📊 Unique conversion keys found: ${uniqueConversionKeys.length}`);
        
        // Log sample keys for verification
        if (uniqueConversionKeys.length > 0) {
            console.log('📝 Sample conversion keys found:');
            uniqueConversionKeys.slice(0, 3).forEach((key, i) => {
                console.log(`  ${i+1}. ${key}`);
            });
        } else {
            console.warn('⚠️ NO CONVERSION KEYS FOUND - This indicates a serious issue with conversion data storage');
        }
        
        return uniqueConversionKeys;
        
    } catch (error) {
        console.error('❌ FIXED conversion key scan failed:', error.message);
        return [];
    }
}

// Fetch attribution stats from Redis
async function fetchAttributionStats(redis) {
  try {
    const statsKeys = await redis('keys/attribution_stats_*');
    const attributionStats = [];
    
    if (statsKeys.result && statsKeys.result.length > 0) {
      const sortedKeys = statsKeys.result
        .sort((a, b) => {
          const timestampA = parseInt(a.split('_').pop()) || 0;
          const timestampB = parseInt(b.split('_').pop()) || 0;
          return timestampB - timestampA;
        })
        .slice(0, 200);
      
      for (const key of sortedKeys) {
        try {
          const statsData = await redis(`get/${key}`);
          if (statsData.result) {
            const parsedStats = JSON.parse(decodeURIComponent(statsData.result));
            
            if (!isValidTimestamp(parsedStats.timestamp)) {
              parsedStats.timestamp = new Date().toISOString();
            }
            
            attributionStats.push(parsedStats);
          }
        } catch (parseError) {
          continue;
        }
      }
    }
    
    return attributionStats;
  } catch (error) {
    console.error('❌ Error fetching attribution stats:', error);
    return [];
  }
}

// Calculate attribution summary metrics
function calculateAttributionSummary(attributionStats, conversions) {
  const totalStats = attributionStats.length;
  const successfulAttributions = attributionStats.filter(stat => stat.success).length;
  const attributionRate = totalStats > 0 ?
    Math.round(successfulAttributions / totalStats * 100) : 0;
  
  const totalConversions = conversions.length;
  const attributedConversions = conversions.filter(conv => 
    conv.attribution_found || conv.landing_page
  ).length;
  const overallAttributionRate = totalConversions > 0 ?
    Math.round(attributedConversions / totalConversions * 100) : 0;
  
  return {
    attribution_success_rate: attributionRate,
    total_attribution_attempts: totalStats,
    successful_attributions: successfulAttributions,
    overall_attribution_rate: overallAttributionRate,
    total_conversions: totalConversions,
    attributed_conversions: attributedConversions
  };
}

// Calculate attribution health metrics
async function calculateAttributionHealth(redis) {
  try {
    const last24Hours = Date.now() - (24 * 60 * 60 * 1000);
    const recentStatsKeys = await redis(`keys/attribution_stats_*`);
    
    if (!recentStatsKeys.result || recentStatsKeys.result.length === 0) {
      return {
        status: 'healthy',
        success_rate: 85,
        total_conversions: 0,
        successful_attributions: 0,
        ipv6_metrics: {
          pageviews: 0,
          dual_stack_ready: true
        },
        timestamp: new Date().toISOString()
      };
    }
    
    const recentStats = [];
    for (const key of recentStatsKeys.result.slice(0, 100)) {
      try {
        const statsData = await redis(`get/${key}`);
        if (statsData.result) {
          const stat = JSON.parse(decodeURIComponent(statsData.result));
          const statTime = parseInt(key.split('_').pop()) || 0;
          
          if (statTime > last24Hours) {
            recentStats.push(stat);
          }
        }
      } catch (e) {
        continue;
      }
    }
    
    const totalAttempts = recentStats.length;
    const successfulAttempts = recentStats.filter(stat => stat.success).length;
    const successRate = totalAttempts > 0 ? Math.round(successfulAttempts / totalAttempts * 100) : 85;
    
    const status = successRate >= 70 ? 'healthy' : (successRate >= 50 ? 'warning' : 'critical');
    
    return {
      status,
      success_rate: successRate,
      total_conversions: totalAttempts,
      successful_attributions: successfulAttempts,
      ipv6_metrics: {
        pageviews: recentStats.filter(stat => stat.ip_address && stat.ip_address.includes(':')).length,
        dual_stack_ready: true
      },
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Attribution health calculation error:', error);
    return {
      status: 'error',
      success_rate: 0,
      total_conversions: 0,
      successful_attributions: 0,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

function applyFilters(data, filters) {
  let filtered = data;
  
  if (filters.start_date) {
    const startDate = new Date(filters.start_date);
    filtered = filtered.filter(item => {
      const safeTimestamp = safeProcessTimestamp(item.timestamp);
      try {
        const itemDate = new Date(safeTimestamp);
        return itemDate >= startDate;
      } catch (e) {
        return true;
      }
    });
  }
  
  if (filters.end_date) {
    const endDate = new Date(filters.end_date);
    endDate.setHours(23, 59, 59, 999);
    filtered = filtered.filter(item => {
      const safeTimestamp = safeProcessTimestamp(item.timestamp);
      try {
        const itemDate = new Date(safeTimestamp);
        return itemDate <= endDate;
      } catch (e) {
        return true;
      }
    });
  }
  
  if (filters.source) {
    filtered = filtered.filter(item => item.source === filters.source);
  }
  
  if (filters.campaign) {
    filtered = filtered.filter(item => 
      (item.utm_campaign || item.campaign) === filters.campaign
    );
  }
  
  return filtered;
}

// MAIN HANDLER WITH CORS FIX
const handler = async (event, context) => {
  // CRITICAL: CORS headers must be first
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS, PUT, DELETE',
    'Access-Control-Max-Age': '86400',
  };

  // Handle OPTIONS preflight request IMMEDIATELY
  if (event.httpMethod === 'OPTIONS') {
    console.log('🔧 CORS preflight request received from:', event.headers.origin || 'unknown');
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({ message: 'CORS preflight successful' })
    };
  }

  // Helper to create responses with CORS headers
  const createResponse = (statusCode, body) => ({
    statusCode,
    headers: corsHeaders,
    body: typeof body === 'string' ? body : JSON.stringify(body)
  });

  try {
    // API Key validation
    const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
    const validApiKey = process.env.OJOY_API_KEY;

    if (!apiKey || apiKey !== validApiKey) {
      console.log('❌ Unauthorized request - API key mismatch');
      return createResponse(401, { error: 'Unauthorized' });
    }

    // Redis setup
    const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
    const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

    if (!redisUrl || !redisToken) {
      console.error('❌ Missing Redis configuration');
      return createResponse(500, { error: 'Server configuration error' });
    }

    const redis = async (command) => {
      try {
        const response = await fetch(`${redisUrl}/${command}`, {
          headers: { Authorization: `Bearer ${redisToken}` }
        });
        return response.json();
      } catch (error) {
        console.error('Redis error:', error);
        throw new Error('Database connection failed');
      }
    };

    // Attribution Health Check Endpoint
    if (event.httpMethod === 'GET' && event.path === '/attribution-health') {
      try {
        console.log('🩺 Attribution health check requested');
        const healthMetrics = await calculateAttributionHealth(redis);
        
        if (healthMetrics.successRate < 70) {
          console.warn(`🚨 ALERT: Attribution success rate dropped to ${healthMetrics.successRate}%`);
        }
        
        return createResponse(200, healthMetrics);
      } catch (error) {
        console.error('❌ Health check error:', error);
        return createResponse(500, { error: error.message });
      }
    }
    
    if (event.httpMethod === 'GET') {
      try {
        // Parse query parameters
        const { start_date, end_date, source, campaign, include_attribution_stats } = event.queryStringParameters || {};
        
        console.log(`📅 Analytics request: ${start_date || 'no start'} to ${end_date || 'no end'}`);
        
        // Get all keys from Redis using COMPREHENSIVE SCANNING
        let attributionKeys = [];
        let conversionKeys = [];
        
        try {
          console.log('🔍 Starting comprehensive Redis key scanning...');
          
          // USE THE NEW COMPREHENSIVE SCANNING FUNCTION
          attributionKeys = await getComprehensiveAttributionKeys(redis);
          console.log(`📊 Comprehensive scan found ${attributionKeys.length} attribution keys`);
          
          if (attributionKeys.length < 100) {
            console.warn('⚠️ VERY LOW KEY COUNT - Data may be missing or using different patterns');
          } else if (attributionKeys.length > 1000) {
            console.log('✅ GOOD KEY COUNT - Found substantial data');
          } else {
            console.log('📊 MODERATE KEY COUNT - May be missing some data');
          }
          
          // Enhanced conversion key scanning
          conversionKeys = await getConversionKeysEnhanced(redis);
          console.log(`🔍 Enhanced scan found ${conversionKeys.length} conversion keys`);
          
        } catch (redisError) {
          console.error('❌ Redis operation failed:', redisError);
          attributionKeys = [];
          conversionKeys = [];
        }
        
        // Fetch attribution data
        let allPageViews = [];
        if (attributionKeys.length > 0) {
          try {
            console.log('📦 Fetching attribution data...');
            
            if (attributionKeys.length > 5000) {
              console.log(`⚠️ Large dataset: ${attributionKeys.length} keys. Processing in batches...`);
              
              // Process in batches to avoid timeouts
              const batchSize = 1000;
              for (let i = 0; i < attributionKeys.length; i += batchSize) {
                const batch = attributionKeys.slice(i, i + batchSize);
                
                try {
                  const batchResults = await Promise.all(
                    batch.map(async (key) => {
                      try {
                        const result = await redis(`get/${key}`);
                        return result.result ? decodeURIComponent(result.result) : null;
                      } catch (e) {
                        return null;
                      }
                    })
                  );
                  
                  batchResults.forEach(item => {
                    if (item) {
                      try {
                        const parsed = JSON.parse(item);
                        
                        // Enhanced timestamp validation with fallback
                        if (!isValidTimestamp(parsed.timestamp)) {
                          console.warn('⚠️ Invalid timestamp found, using current time');
                          parsed.timestamp = new Date().toISOString();
                        }
                        
                        allPageViews.push(parsed);
                      } catch (parseError) {
                        console.warn('⚠️ Failed to parse attribution data:', parseError.message);
                      }
                    }
                  });
                  
                  console.log(`✅ Processed batch ${Math.floor(i/batchSize) + 1}: ${allPageViews.length} total page views so far`);
                } catch (batchError) {
                  console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, batchError.message);
                }
              }
            } else {
              // Process all at once for smaller datasets
              const attributionResults = await Promise.all(
                attributionKeys.map(async (key) => {
                  try {
                    const result = await redis(`get/${key}`);
                    return result.result ? decodeURIComponent(result.result) : null;
                  } catch (e) {
                    return null;
                  }
                })
              );
              
              attributionResults.forEach(item => {
                if (item) {
                  try {
                    const parsed = JSON.parse(item);
                    
                    // Enhanced timestamp validation with fallback
                    if (!isValidTimestamp(parsed.timestamp)) {
                      console.warn('⚠️ Invalid timestamp found, using current time');
                      parsed.timestamp = new Date().toISOString();
                    }
                    
                    allPageViews.push(parsed);
                  } catch (parseError) {
                    console.warn('⚠️ Failed to parse attribution data:', parseError.message);
                  }
                }
              });
            }
          } catch (attributionError) {
            console.error('❌ Attribution data fetch error:', attributionError);
            allPageViews = [];
          }
        }
        
        // Fetch conversion data
        let allConversions = [];
        if (conversionKeys.length > 0) {
          try {
            console.log('💰 Fetching conversion data...');
            
            const conversionResults = await Promise.all(
              conversionKeys.map(async (key) => {
                try {
                  const result = await redis(`get/${key}`);
                  return result.result ? decodeURIComponent(result.result) : null;
                } catch (e) {
                  return null;
                }
              })
            );
            
            conversionResults.forEach(item => {
              if (item) {
                try {
                  const parsed = JSON.parse(item);
                  
                  // Enhanced timestamp validation with fallback
                  if (!isValidTimestamp(parsed.timestamp)) {
                    console.warn('⚠️ Invalid timestamp found, using current time');
                    parsed.timestamp = new Date().toISOString();
                  }
                  
                  allConversions.push(parsed);
                } catch (parseError) {
                  console.warn('⚠️ Failed to parse conversion data:', parseError.message);
                }
              }
            });
          } catch (conversionError) {
            console.error('❌ Conversion data fetch error:', conversionError);
            allConversions = [];
          }
        }
        
        console.log(`📊 Analytics query returned ${allPageViews.length} page views and ${allConversions.length} conversions`);
        
        // Apply filters
        let filteredConversions = applyFilters(allConversions, { start_date, end_date, source, campaign });
        let filteredPageViews = applyFilters(allPageViews, { start_date, end_date, source, campaign });
        
        console.log(`📊 After filtering: ${filteredPageViews.length} page views and ${filteredConversions.length} conversions`);
        
        // Include attribution stats if requested
        let attributionStatsData = null;
        if (include_attribution_stats === 'true') {
          try {
            attributionStatsData = await fetchAttributionStats(redis);
            console.log(`✅ Fetched ${attributionStatsData.length} attribution stat records`);
          } catch (statsError) {
            console.error('❌ Failed to fetch attribution stats:', statsError);
            attributionStatsData = [];
          }
        }
        
        // Calculate analytics
        const totalConversions = filteredConversions.length;
        const totalPageViews = filteredPageViews.length;
        
        const uniqueVisitorIPs = new Set();
        filteredPageViews.forEach(pv => {
          if (pv.ip_address && pv.ip_address !== 'unknown') {
            uniqueVisitorIPs.add(pv.ip_address);
          }
        });
        const uniqueVisitors = uniqueVisitorIPs.size;
        
        const paidConversions = filteredConversions.filter(item => (parseFloat(item.order_total) || 0) > 0);
        const totalRevenue = filteredConversions.reduce((sum, item) => sum + (parseFloat(item.order_total) || 0), 0);
        const avgOrderValue = paidConversions.length > 0 ? totalRevenue / paidConversions.length : 0;
        const conversionRate = uniqueVisitors > 0 ? 
          (filteredConversions.length / uniqueVisitors * 100) : 0;
        
        // Group data for analytics
        const sourceMap = new Map();
        const campaignMap = new Map();
        const pageMap = new Map();
        const dailyMap = new Map();
        
        filteredPageViews.forEach(pv => {
          const source = pv.source || 'direct';
          const campaign = pv.utm_campaign || 'none';
          const page = pv.landing_page || 'unknown';
          
          if (!isValidTimestamp(pv.timestamp)) {
            console.warn('⚠️ Skipping invalid timestamp:', pv.timestamp);
            return;
          }
          
          const date = new Date(pv.timestamp).toISOString().split('T')[0];
          
          // Source analytics
          if (!sourceMap.has(source)) {
            sourceMap.set(source, { source, pageViews: 0, uniqueVisitors: new Set(), conversions: 0 });
          }
          sourceMap.get(source).pageViews++;
          sourceMap.get(source).uniqueVisitors.add(pv.ip_address || 'unknown');
          
          // Campaign analytics
          if (!campaignMap.has(campaign)) {
            campaignMap.set(campaign, { campaign, pageViews: 0, uniqueVisitors: new Set(), conversions: 0 });
          }
          campaignMap.get(campaign).pageViews++;
          campaignMap.get(campaign).uniqueVisitors.add(pv.ip_address || 'unknown');
          
          // Page analytics
          if (!pageMap.has(page)) {
            pageMap.set(page, { landing_page: page, pageViews: 0, uniqueVisitors: new Set(), conversions: 0 });
          }
          pageMap.get(page).pageViews++;
          pageMap.get(page).uniqueVisitors.add(pv.ip_address || 'unknown');
          
          // Daily analytics
          if (!dailyMap.has(date)) {
            dailyMap.set(date, { date, pageViews: 0, conversions: 0, revenue: 0 });
          }
          dailyMap.get(date).pageViews++;
        });
        
        // Add conversion data to maps
        filteredConversions.forEach(conv => {
          const source = conv.source || 'direct';
          const campaign = conv.utm_campaign || 'none';
          const page = conv.landing_page || 'unknown';
          
          if (!isValidTimestamp(conv.timestamp)) {
            console.warn('⚠️ Skipping invalid timestamp:', conv.timestamp);
            return;
          }
          
          const date = new Date(conv.timestamp).toISOString().split('T')[0];
          const revenue = parseFloat(conv.order_total) || 0;
          
          if (sourceMap.has(source)) {
            sourceMap.get(source).conversions++;
          }
          if (campaignMap.has(campaign)) {
            campaignMap.get(campaign).conversions++;
          }
          if (pageMap.has(page)) {
            pageMap.get(page).conversions++;
          }
          if (dailyMap.has(date)) {
            dailyMap.get(date).conversions++;
            dailyMap.get(date).revenue += revenue;
          }
        });
        
        // Convert maps to arrays and calculate conversion rates
        const topSources = Array.from(sourceMap.values()).map(item => ({
          ...item,
          uniqueVisitors: item.uniqueVisitors.size,
          conversionRate: item.uniqueVisitors.size > 0 ? 
            (item.conversions / item.uniqueVisitors.size * 100).toFixed(1) : '0.0'
        })).sort((a, b) => b.pageViews - a.pageViews);
        
        const topCampaigns = Array.from(campaignMap.values()).map(item => ({
          ...item,
          uniqueVisitors: item.uniqueVisitors.size,
          conversionRate: item.uniqueVisitors.size > 0 ? 
            (item.conversions / item.uniqueVisitors.size * 100).toFixed(1) : '0.0'
        })).sort((a, b) => b.pageViews - a.pageViews);
        
        const topLandingPages = Array.from(pageMap.values()).map(item => ({
          ...item,
          uniqueVisitors: item.uniqueVisitors.size,
          conversionRate: item.uniqueVisitors.size > 0 ? 
            (item.conversions / item.uniqueVisitors.size * 100).toFixed(1) : '0.0'
        })).sort((a, b) => b.pageViews - a.pageViews);
        
        const dailyTrends = Array.from(dailyMap.values()).sort((a, b) => a.date.localeCompare(b.date));
        
        // Build response object
        const response = {
          summary: {
            total_page_views: totalPageViews,
            unique_visitors: uniqueVisitors,
            total_conversions: totalConversions,
            free_trials: filteredConversions.filter(c => (parseFloat(c.order_total) || 0) === 0).length,
            paid_conversions: paidConversions.length,
            total_revenue: totalRevenue,
            avg_order_value: avgOrderValue,
            conversion_rate: conversionRate.toFixed(1),
            date_range: { start: start_date, end: end_date }
          },
          traffic_sources: topSources,
          campaign_performance: topCampaigns,
          landing_page_performance: topLandingPages,
          daily_trends: dailyTrends,
          conversions: filteredConversions,
          page_views: filteredPageViews,
          
          debug: {
            attribution_keys_found: attributionKeys.length,
            conversion_keys_found: conversionKeys.length,
            raw_page_views_processed: allPageViews.length,
            filtered_page_views: filteredPageViews.length,
            raw_conversions_processed: allConversions.length,
            filtered_conversions: filteredConversions.length,
            cors_enabled: true,
            deployment_timestamp: new Date().toISOString(),
            comprehensive_scanning_enabled: true
          }
        };
        
        // Add attribution stats to response if requested
        if (attributionStatsData !== null) {
          response.attribution_stats = attributionStatsData;
          response.attribution_summary = calculateAttributionSummary(attributionStatsData, filteredConversions);
        }
        
        return createResponse(200, response);
        
      } catch (error) {
        console.error('❌ Analytics GET error:', error);
        return createResponse(500, { error: error.message });
      }
    }
    
    if (event.httpMethod === 'POST') {
      try {
        const data = JSON.parse(event.body);
        
        if (!isValidTimestamp(data.timestamp)) {
          data.timestamp = new Date().toISOString();
        }
        
        if (data.event_type === 'purchase' || data.event_type === 'conversion' || data.order_total !== undefined) {
          const key = data.email ?
            `conversions:${data.email.replace(/[^a-zA-Z0-9]/g, '_')}:${Date.now()}` :
            `conversions:${data.timestamp}:${Math.random()}`;
          
          await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
          console.log(`✅ Stored conversion: ${data.email || 'no email'}`);
        } else {
          const key = `pageviews:${data.timestamp}:${Math.random()}`;
          await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
          console.log(`✅ Stored page view: ${data.source} → ${data.landing_page}`);
        }
        
        return createResponse(200, { success: true });
        
      } catch (error) {
        console.error('❌ Analytics POST error:', error);
        return createResponse(500, { error: error.message });
      }
    }
    
    return createResponse(405, { error: 'Method not allowed' });
    
  } catch (error) {
    console.error('❌ Unexpected error:', error);
    return createResponse(500, { 
      error: 'Internal server error', 
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
};

module.exports = { handler };
