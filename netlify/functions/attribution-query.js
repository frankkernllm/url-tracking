// Multi-Touch Attribution Query Engine
// Path: netlify/functions/attribution-query.js
// Purpose: Perform first-touch/last-touch attribution analysis using multi-index system

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    // Parse query parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const queryParams = event.queryStringParameters || {};
    
    // Extract attribution query parameters
    const attributionQuery = {
      start_date: body.start_date || queryParams.start_date,
      end_date: body.end_date || queryParams.end_date,
      attribution_model: body.attribution_model || queryParams.attribution_model || 'first_touch',
      lookback_days: parseInt(body.lookback_days || queryParams.lookback_days || '14'),
      
      // Optional filters
      source: body.source || queryParams.source,
      campaign: body.campaign || queryParams.campaign,
      landing_page: body.landing_page || queryParams.landing_page,
      
      // Debug options
      debug: body.debug === 'true' || queryParams.debug === 'true',
      sample_size: parseInt(body.sample_size || queryParams.sample_size || '0') // 0 = no limit
    };
    
    console.log('🚀 ATTRIBUTION QUERY starting with parameters:', attributionQuery);
    
    // Validate required parameters
    if (!attributionQuery.start_date || !attributionQuery.end_date) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({
          error: 'Missing required parameters',
          message: 'start_date and end_date are required',
          example: {
            start_date: '2025-07-01',
            end_date: '2025-07-31',
            attribution_model: 'first_touch'
          }
        })
      };
    }
    
    // Validate attribution model
    if (!['first_touch', 'last_touch'].includes(attributionQuery.attribution_model)) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({
          error: 'Invalid attribution model',
          message: 'attribution_model must be "first_touch" or "last_touch"'
        })
      };
    }
    
    // Step 1: Get conversions for the specified date range
    console.log('🔍 Step 1: Fetching conversions for date range...');
    const conversionsResult = await getConversionsForDateRange(redis, attributionQuery);
    
    if (conversionsResult.conversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          attribution_summary: {
            model: attributionQuery.attribution_model,
            date_range: {
              start: attributionQuery.start_date,
              end: attributionQuery.end_date
            },
            total_conversions: 0,
            attributed_conversions: 0,
            attribution_rate: '0%'
          },
          attribution_table: [],
          message: 'No conversions found for the specified date range'
        })
      };
    }
    
    console.log(`📊 Found ${conversionsResult.conversions.length} conversions to analyze`);
    
    // Step 2: Perform attribution analysis
    console.log('🎯 Step 2: Performing attribution analysis...');
    const attributionResult = await performAttributionAnalysis(
      redis, 
      conversionsResult.conversions, 
      attributionQuery
    );
    
    // Step 3: Build attribution table and summary
    console.log('📈 Step 3: Building attribution table...');
    const attributionTable = buildAttributionTable(attributionResult.attributedConversions, attributionQuery);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Attribution analysis completed in ${totalTime}ms`);
    
    // Build comprehensive response
    const response = {
      success: true,
      attribution_summary: {
        model: attributionQuery.attribution_model,
        date_range: {
          start: attributionQuery.start_date,
          end: attributionQuery.end_date,
          lookback_days: attributionQuery.lookback_days
        },
        total_conversions: conversionsResult.conversions.length,
        attributed_conversions: attributionResult.attributedConversions.length,
        attribution_rate: conversionsResult.conversions.length > 0 
          ? `${((attributionResult.attributedConversions.length / conversionsResult.conversions.length) * 100).toFixed(1)}%`
          : '0%',
        total_revenue: attributionResult.totalRevenue.toFixed(2),
        attributed_revenue: attributionResult.attributedRevenue.toFixed(2),
        processing_time_ms: totalTime
      },
      attribution_table: attributionTable,
      attribution_methods_used: attributionResult.attributionMethodsUsed,
      filters_applied: {
        source: attributionQuery.source || null,
        campaign: attributionQuery.campaign || null,
        landing_page: attributionQuery.landing_page || null
      }
    };
    
    // Add debug information if requested
    if (attributionQuery.debug) {
      response.debug_info = {
        conversions_processed: conversionsResult.conversions.length,
        attribution_attempts: attributionResult.attributionAttempts,
        matching_statistics: attributionResult.matchingStats,
        sample_conversions: attributionResult.attributedConversions.slice(0, 5).map(conv => ({
          email: conv.email,
          order_total: conv.order_total,
          timestamp: conv.timestamp,
          attribution_method: conv.attribution_method,
          attributed_landing_page: conv.attributed_landing_page,
          attributed_source: conv.attributed_source
        }))
      };
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(response)
    };
    
  } catch (error) {
    console.error('❌ Attribution query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Attribution query failed', 
        message: error.message 
      })
    };
  }
};

// Get conversions for specified date range
async function getConversionsForDateRange(redis, query) {
  console.log(`🔍 Fetching conversions from ${query.start_date} to ${query.end_date}...`);
  
  const startDate = new Date(query.start_date);
  const endDate = new Date(query.end_date);
  endDate.setHours(23, 59, 59, 999); // End of day
  
  let cursor = '0';
  let allConversions = [];
  let keysScanned = 0;
  
  try {
    // Scan for conversion keys
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      
      console.log(`📦 Conversion scan: Found ${keys.length} keys, cursor: ${cursor}`);
      
      // Process keys in batches
      if (keys.length > 0) {
        const batchSize = 50;
        for (let i = 0; i < keys.length; i += batchSize) {
          const batch = keys.slice(i, i + batchSize);
          
          const batchResults = await Promise.all(
            batch.map(async (key) => {
              try {
                const result = await redis(`get/${key}`, 1500);
                if (result?.result) {
                  let parsed;
                  try {
                    parsed = JSON.parse(result.result);
                  } catch (parseError) {
                    try {
                      parsed = JSON.parse(decodeURIComponent(result.result));
                    } catch (decodeError) {
                      return null;
                    }
                  }
                  
                  if (parsed && parsed.timestamp) {
                    const conversionDate = new Date(parsed.timestamp);
                    
                    // Check if conversion is within date range
                    if (conversionDate >= startDate && conversionDate <= endDate) {
                      return {
                        ...parsed,
                        redis_key: key,
                        conversion_date: conversionDate
                      };
                    }
                  }
                }
              } catch (error) {
                // Skip failed conversions
              }
              return null;
            })
          );
          
          const validConversions = batchResults.filter(conv => conv !== null);
          allConversions.push(...validConversions);
        }
      }
      
    } while (cursor !== '0' && keysScanned < 10000); // Safety limit
    
    // Apply sample size limit if specified
    if (query.sample_size > 0 && allConversions.length > query.sample_size) {
      console.log(`📊 Applying sample size limit: ${allConversions.length} -> ${query.sample_size}`);
      allConversions = allConversions.slice(0, query.sample_size);
    }
    
    console.log(`✅ Conversions fetch complete: ${allConversions.length} conversions, ${keysScanned} keys scanned`);
    
    return {
      conversions: allConversions,
      keys_scanned: keysScanned
    };
    
  } catch (error) {
    console.error('❌ Conversion fetching failed:', error);
    return { conversions: [], keys_scanned: keysScanned };
  }
}

// Perform attribution analysis for conversions
async function performAttributionAnalysis(redis, conversions, query) {
  console.log(`🎯 Starting ${query.attribution_model} attribution analysis for ${conversions.length} conversions...`);
  
  const attributedConversions = [];
  const attributionAttempts = {
    session_matches: 0,
    ip_matches: 0,
    fingerprint_matches: 0,
    no_matches: 0
  };
  
  const matchingStats = {
    total_attempts: 0,
    successful_attributions: 0,
    failed_attributions: 0
  };
  
  const attributionMethodsUsed = new Set();
  let totalRevenue = 0;
  let attributedRevenue = 0;
  
  // Process conversions in batches for performance
  const batchSize = 20;
  for (let i = 0; i < conversions.length; i += batchSize) {
    const batch = conversions.slice(i, i + batchSize);
    console.log(`🔄 Processing attribution batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(conversions.length/batchSize)}`);
    
    const batchResults = await Promise.all(
      batch.map(async (conversion) => {
        matchingStats.total_attempts++;
        totalRevenue += parseFloat(conversion.order_total) || 0;
        
        try {
          // Find attribution for this conversion
          const attribution = await findAttributionForConversion(redis, conversion, query);
          
          if (attribution) {
            matchingStats.successful_attributions++;
            attributedRevenue += parseFloat(conversion.order_total) || 0;
            attributionMethodsUsed.add(attribution.method);
            
            // Track method statistics
            if (attribution.method.includes('session')) {
              attributionAttempts.session_matches++;
            } else if (attribution.method.includes('ip')) {
              attributionAttempts.ip_matches++;
            } else if (attribution.method.includes('fingerprint')) {
              attributionAttempts.fingerprint_matches++;
            }
            
            return {
              ...conversion,
              attribution_found: true,
              attribution_method: attribution.method,
              attributed_landing_page: attribution.landing_page,
              attributed_source: attribution.source,
              attributed_campaign: attribution.campaign,
              attributed_pageview: attribution.pageview,
              attribution_timestamp: attribution.pageview.timestamp
            };
          } else {
            matchingStats.failed_attributions++;
            attributionAttempts.no_matches++;
            return null;
          }
          
        } catch (error) {
          console.log(`⚠️ Attribution error for conversion ${conversion.email}: ${error.message}`);
          matchingStats.failed_attributions++;
          attributionAttempts.no_matches++;
          return null;
        }
      })
    );
    
    const validResults = batchResults.filter(result => result !== null);
    attributedConversions.push(...validResults);
  }
  
  console.log(`✅ Attribution analysis complete:`);
  console.log(`   🎯 Total conversions: ${conversions.length}`);
  console.log(`   ✅ Attributed conversions: ${attributedConversions.length}`);
  console.log(`   📊 Attribution rate: ${((attributedConversions.length / conversions.length) * 100).toFixed(1)}%`);
  console.log(`   🔗 Session matches: ${attributionAttempts.session_matches}`);
  console.log(`   🌐 IP matches: ${attributionAttempts.ip_matches}`);
  console.log(`   🖱️ Fingerprint matches: ${attributionAttempts.fingerprint_matches}`);
  console.log(`   ❌ No matches: ${attributionAttempts.no_matches}`);
  
  return {
    attributedConversions,
    attributionAttempts,
    matchingStats,
    attributionMethodsUsed: Array.from(attributionMethodsUsed),
    totalRevenue,
    attributedRevenue
  };
}

// Find attribution for a single conversion
async function findAttributionForConversion(redis, conversion, query) {
  console.log(`🔍 Finding attribution for conversion: ${conversion.email} at ${conversion.timestamp}`);
  
  const conversionDate = new Date(conversion.timestamp);
  const lookbackStart = new Date(conversionDate.getTime() - (query.lookback_days * 24 * 60 * 60 * 1000));
  
  // Priority 1: Session ID matching (highest priority)
  if (conversion.ssid || conversion.session_id) {
    const sessionId = conversion.ssid || conversion.session_id;
    console.log(`🔗 Attempting session ID match: ${sessionId}`);
    
    try {
      const sessionKey = `attribution_index_v1_session:${sessionId}`;
      const sessionResult = await redis(`get/${sessionKey}`, 2000);
      
      if (sessionResult?.result) {
        const sessionIndex = JSON.parse(decodeURIComponent(sessionResult.result));
        
        if (sessionIndex.pageviews && sessionIndex.pageviews.length > 0) {
          // Filter pageviews within lookback window
          const validPageviews = sessionIndex.pageviews.filter(pv => {
            const pvDate = new Date(pv.timestamp);
            return pvDate >= lookbackStart && pvDate <= conversionDate;
          });
          
          if (validPageviews.length > 0) {
            const attributedPageview = query.attribution_model === 'first_touch' 
              ? validPageviews[validPageviews.length - 1] // Oldest first (sorted desc, so last item)
              : validPageviews[0]; // Most recent first
            
            console.log(`✅ Session attribution found: ${attributedPageview.landing_page} via ${attributedPageview.source}`);
            
            return {
              method: 'session_id_match',
              landing_page: attributedPageview.landing_page,
              source: attributedPageview.source,
              campaign: attributedPageview.utm_campaign,
              pageview: attributedPageview
            };
          }
        }
      }
    } catch (error) {
      console.log(`⚠️ Session lookup failed: ${error.message}`);
    }
  }
  
  // Priority 2: IP Address matching
  const conversionIPs = extractConversionIPs(conversion);
  
  for (const ip of conversionIPs) {
    if (!ip || ip === 'unknown') continue;
    
    console.log(`🌐 Attempting IP match: ${ip.substring(0, 20)}...`);
    
    try {
      const encodedIP = encodeIPForKey(ip);
      const ipKey = `attribution_index_v1_ip:${encodedIP}`;
      const ipResult = await redis(`get/${ipKey}`, 2000);
      
      if (ipResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(ipResult.result));
        
        if (ipIndex.pageviews && ipIndex.pageviews.length > 0) {
          const validPageviews = ipIndex.pageviews.filter(pv => {
            const pvDate = new Date(pv.timestamp);
            return pvDate >= lookbackStart && pvDate <= conversionDate;
          });
          
          if (validPageviews.length > 0) {
            const attributedPageview = query.attribution_model === 'first_touch' 
              ? validPageviews[validPageviews.length - 1] 
              : validPageviews[0];
            
            console.log(`✅ IP attribution found: ${attributedPageview.landing_page} via ${attributedPageview.source}`);
            
            return {
              method: 'ip_address_match',
              landing_page: attributedPageview.landing_page,
              source: attributedPageview.source,
              campaign: attributedPageview.utm_campaign,
              pageview: attributedPageview
            };
          }
        }
      }
    } catch (error) {
      console.log(`⚠️ IP lookup failed for ${ip}: ${error.message}`);
    }
  }
  
  // Priority 3: Fingerprint matching (canvas, webgl)
  if (conversion.dsig || conversion.device_signature) {
    const deviceSig = conversion.dsig || conversion.device_signature;
    console.log(`🖱️ Attempting fingerprint match: ${deviceSig.substring(0, 20)}...`);
    
    // For fingerprint matching, we need to scan through attribution data
    // This is more expensive but provides fallback attribution
    try {
      const fingerprintAttribution = await findFingerprintAttribution(
        redis, 
        deviceSig, 
        conversionDate, 
        lookbackStart, 
        query.attribution_model
      );
      
      if (fingerprintAttribution) {
        console.log(`✅ Fingerprint attribution found: ${fingerprintAttribution.landing_page}`);
        return fingerprintAttribution;
      }
    } catch (error) {
      console.log(`⚠️ Fingerprint lookup failed: ${error.message}`);
    }
  }
  
  console.log(`❌ No attribution found for conversion: ${conversion.email}`);
  return null;
}

// Extract conversion IP addresses (handles multiple formats)
function extractConversionIPs(conversion) {
  const ips = [];
  
  // Various IP fields in conversions
  if (conversion.conversion_ip) ips.push(conversion.conversion_ip);
  if (conversion.primary_ip) ips.push(conversion.primary_ip);
  if (conversion.ip_address) {
    // Handle comma-separated IPs
    if (conversion.ip_address.includes(',')) {
      ips.push(...conversion.ip_address.split(',').map(ip => ip.trim()));
    } else {
      ips.push(conversion.ip_address);
    }
  }
  if (conversion.CIP) ips.push(conversion.CIP);
  if (conversion.PIP) ips.push(conversion.PIP);
  if (conversion.IP) ips.push(conversion.IP);
  
  // Handle unique_ips array format
  if (conversion.unique_ips && Array.isArray(conversion.unique_ips)) {
    ips.push(...conversion.unique_ips);
  }
  
  // Remove duplicates and invalid IPs
  return [...new Set(ips)].filter(ip => ip && ip !== 'unknown' && ip !== 'null');
}

// Find attribution based on fingerprint matching
async function findFingerprintAttribution(redis, deviceSignature, conversionDate, lookbackStart, attributionModel) {
  // This is a simplified fingerprint search - in production you might want to optimize further
  console.log(`🖱️ Performing fingerprint attribution search...`);
  
  try {
    // Look for attribution data chunks and search for matching fingerprints
    let cursor = '0';
    let searchAttempts = 0;
    const maxSearchAttempts = 5; // Limit search to avoid timeout
    
    do {
      if (searchAttempts >= maxSearchAttempts) break;
      
      const scanResult = await redis(`scan/${cursor}/match/attribution_data_chunk:v1_*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      searchAttempts++;
      
      // Check first few chunks for fingerprint matches
      for (const key of keys.slice(0, 2)) {
        try {
          const chunkData = await redis(`get/${key}`, 1500);
          if (chunkData?.result) {
            const chunk = JSON.parse(decodeURIComponent(chunkData.result));
            
            if (chunk.pageviews) {
              const matchingPageviews = chunk.pageviews.filter(pv => {
                const pvDate = new Date(pv.timestamp);
                return pvDate >= lookbackStart && 
                       pvDate <= conversionDate &&
                       (pv.canvas_fingerprint === deviceSignature || 
                        pv.webgl_fingerprint === deviceSignature);
              });
              
              if (matchingPageviews.length > 0) {
                const attributedPageview = attributionModel === 'first_touch' 
                  ? matchingPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp))[0]
                  : matchingPageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];
                
                return {
                  method: 'fingerprint_match',
                  landing_page: attributedPageview.landing_page,
                  source: attributedPageview.source,
                  campaign: attributedPageview.utm_campaign,
                  pageview: attributedPageview
                };
              }
            }
          }
        } catch (error) {
          // Continue searching other chunks
        }
      }
      
    } while (cursor !== '0' && searchAttempts < maxSearchAttempts);
    
  } catch (error) {
    console.log(`⚠️ Fingerprint search error: ${error.message}`);
  }
  
  return null;
}

// Build attribution table from attributed conversions
function buildAttributionTable(attributedConversions, query) {
  console.log(`📈 Building attribution table for ${attributedConversions.length} attributed conversions...`);
  
  const attributionMap = new Map();
  
  // Group conversions by landing page + source combination
  attributedConversions.forEach(conversion => {
    const key = `${conversion.attributed_landing_page}|||${conversion.attributed_source}`;
    
    if (!attributionMap.has(key)) {
      attributionMap.set(key, {
        landing_page: conversion.attributed_landing_page,
        source: conversion.attributed_source,
        campaign: conversion.attributed_campaign || 'none',
        conversions: 0,
        revenue: 0,
        conversion_emails: [],
        attribution_methods: new Set()
      });
    }
    
    const entry = attributionMap.get(key);
    entry.conversions++;
    entry.revenue += parseFloat(conversion.order_total) || 0;
    entry.conversion_emails.push(conversion.email);
    entry.attribution_methods.add(conversion.attribution_method);
  });
  
  // Convert to array and calculate additional metrics
  const attributionTable = Array.from(attributionMap.values()).map(entry => ({
    landing_page: entry.landing_page,
    source: entry.source,
    campaign: entry.campaign,
    conversions: entry.conversions,
    revenue: parseFloat(entry.revenue.toFixed(2)),
    attribution_methods: Array.from(entry.attribution_methods),
    conversion_rate: 'N/A', // Would need unique pageviews to calculate
    avg_order_value: parseFloat((entry.revenue / entry.conversions).toFixed(2))
  }));
  
  // Sort by revenue descending
  attributionTable.sort((a, b) => b.revenue - a.revenue);
  
  console.log(`✅ Attribution table built with ${attributionTable.length} entries`);
  
  return attributionTable;
}

// Utility function for encoding IPs
function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_').replace(/\./g, '_');
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
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
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };
}
