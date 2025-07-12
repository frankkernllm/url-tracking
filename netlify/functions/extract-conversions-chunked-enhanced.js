// extract-conversions-chunked-enhanced.js - Build conversion indexes with ALL IP data
// Path: netlify/functions/extract-conversions-chunked-enhanced.js
// ENHANCED: Captures all IP data stored by track.js

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  try {
    console.log('💰 ENHANCED CONVERSION EXTRACTOR: Starting with ALL IP data capture...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Step 1: Find all conversion keys
    const conversionKeys = await findAllConversionKeys(redis);
    console.log(`📊 Found ${conversionKeys.length} conversion keys`);
    
    if (conversionKeys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No conversion keys found'
        })
      };
    }
    
    // Step 2: Load and parse all conversions with ENHANCED IP extraction
    const allConversions = await loadAllConversionsEnhanced(redis, conversionKeys, maxProcessingTime - (Date.now() - startTime));
    console.log(`💰 Loaded ${allConversions.length} conversions with complete IP data`);
    
    // Step 3: Build enhanced date-based indexes
    const indexResults = await buildEnhancedConversionDateIndexes(redis, allConversions, maxProcessingTime - (Date.now() - startTime));
    
    // Step 4: Build IP-specific analytics indexes
    const ipAnalyticsResults = await buildIPAnalyticsIndexes(redis, allConversions, maxProcessingTime - (Date.now() - startTime));
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Enhanced conversion indexing complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        enhanced_conversion_indexing_summary: {
          conversion_keys_found: conversionKeys.length,
          conversions_loaded: allConversions.length,
          date_indexes_created: indexResults.date_indexes_created,
          ip_analytics_indexes_created: ipAnalyticsResults.ip_indexes_created,
          dual_ip_conversions_found: ipAnalyticsResults.dual_ip_count,
          ipv6_conversions_found: ipAnalyticsResults.ipv6_count,
          date_range_covered: indexResults.date_range,
          processing_time_ms: totalTime
        },
        ip_analytics: {
          unique_primary_ips: ipAnalyticsResults.unique_primary_ips,
          unique_conversion_ips: ipAnalyticsResults.unique_conversion_ips,
          extraction_methods_used: ipAnalyticsResults.extraction_methods,
          dual_ip_percentage: ipAnalyticsResults.dual_ip_percentage
        },
        indexing_performance: {
          conversions_per_second: Math.round(allConversions.length / (totalTime / 1000)),
          indexes_per_second: Math.round((indexResults.date_indexes_created + ipAnalyticsResults.ip_indexes_created) / (totalTime / 1000))
        },
        next_steps: [
          'Test enhanced fast-analytics endpoint',
          'Verify conversion_index_date:* and ip_analytics:* keys exist in Redis',
          'Use IP analytics for dual-stack analysis'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Enhanced conversion extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Enhanced conversion extraction failed', 
        message: error.message 
      })
    };
  }
};

// Find all conversion keys in Redis
async function findAllConversionKeys(redis) {
  console.log('🔍 Scanning for conversion keys...');
  
  const conversionKeys = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      conversionKeys.push(...keys);
      iterations++;
      
      if (conversionKeys.length % 500 === 0) {
        console.log(`📊 Conversion scan progress: ${conversionKeys.length} keys found`);
      }
      
    } catch (scanError) {
      console.log(`⚠️ Conversion scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  console.log(`✅ Conversion scan complete: ${conversionKeys.length} keys found`);
  return conversionKeys;
}

// ENHANCED: Load all conversions with complete IP data extraction
async function loadAllConversionsEnhanced(redis, conversionKeys, maxTime) {
  const loadStartTime = Date.now();
  const conversions = [];
  
  console.log(`💰 Loading ${conversionKeys.length} conversions with ENHANCED IP extraction...`);
  
  const batchSize = 50;
  for (let i = 0; i < conversionKeys.length; i += batchSize) {
    if (Date.now() - loadStartTime > maxTime - 3000) {
      console.log(`⏰ Time limit reached while loading conversions`);
      break;
    }
    
    const batch = conversionKeys.slice(i, i + batchSize);
    
    try {
      const batchPromises = batch.map(async (key) => {
        try {
          const conversionData = await redis(`get/${key}`);
          if (conversionData?.result) {
            const conversion = JSON.parse(decodeURIComponent(conversionData.result));
            
            // Validate conversion has required fields
            if (conversion.timestamp && conversion.email) {
              return {
                // Core conversion data
                timestamp: conversion.timestamp,
                email: conversion.email,
                order_total: conversion.order_total || 0,
                order_id: conversion.order_id,
                event_type: conversion.event_type,
                
                // Attribution data
                attribution_found: conversion.attribution_found || false,
                attribution_method: conversion.attribution_method,
                attribution_score: conversion.attribution_score || 0,
                source: conversion.source,
                campaign: conversion.campaign,
                medium: conversion.medium,
                landing_page: conversion.landing_page,
                
                // ENHANCED: Complete IP data extraction with CLEAR ATTRIBUTION ANALYSIS LABELING
                // Main IP addresses for pageview attribution matching
                main_ip_address: conversion.ip_address,              // Primary IP chosen for attribution lookup
                primary_ip: conversion.primary_ip || null,           // PIP: Top-level webhook IP (often IPv6)
                conversion_ip: conversion.conversion_ip || null,     // CIP: Deep-nested extraction IP (often IPv4)
                pageview_ip: conversion.pageview_ip || null,         // IP: Fallback IP (usually same as CIP)
                
                // Attribution success analysis
                attribution_method: conversion.attribution_method,   // Which IP method succeeded
                winning_ip_value: getWinningIPValue(conversion),     // Actual IP that found attribution
                winning_ip_type: getIPType(getWinningIPValue(conversion)), // IPv4 or IPv6
                attempted_ip_addresses: conversion.unique_ips || [], // All IPs that could be tried
                
                // IP scenario classification
                dual_ip_scenario: conversion.dual_ip_scenario || false,
                ip_addresses_detected: conversion.ip_addresses_detected || 1,
                ip_version_mix: analyzeIPVersionMix(conversion),     // "ipv4_only", "ipv6_only", "dual_stack"
                ip_extraction_methods: conversion.ip_extraction_methods || {},
                
                // Pageview matching analysis fields
                pageview_lookup_priority: determinePageviewLookupPriority(conversion),
                potential_pageview_keys: generatePotentialPageviewKeys(conversion),
                
                // Attribution tracking variables (for recovery analysis)
                session_id: conversion.session_id,
                ssid: conversion.ssid,
                device_signature: conversion.device_signature,
                dsig: conversion.dsig,
                screen_value: conversion.screen_value,
                SVV: conversion.SVV,
                SVVV: conversion.SVVV,
                gpu_signature: conversion.gpu_signature,
                gsig: conversion.gsig,
                
                // Metadata
                _redis_key: key,
                _extraction_timestamp: new Date().toISOString()
              };
            }
          }
        } catch (parseError) {
          console.warn(`⚠️ Failed to parse conversion ${key}: ${parseError.message}`);
        }
        return null;
      });
      
      const batchResults = await Promise.all(batchPromises);
      const validResults = batchResults.filter(result => result !== null);
      conversions.push(...validResults);
      
      if ((i + batchSize) % 200 === 0) {
        console.log(`💰 Loading progress: ${conversions.length} conversions loaded from ${i + batchSize}/${conversionKeys.length} keys`);
      }
      
    } catch (batchError) {
      console.log(`⚠️ Error loading conversion batch ${Math.floor(i/batchSize) + 1}:`, batchError.message);
    }
  }
  
  // Sort by timestamp for consistent processing
  conversions.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  // Log IP analytics summary
  const dualIPCount = conversions.filter(c => c.dual_ip_scenario).length;
  const ipv6Count = conversions.filter(c => 
    (c.primary_ip && c.primary_ip.includes(':')) || 
    (c.conversion_ip && c.conversion_ip.includes(':')) || 
    (c.pageview_ip && c.pageview_ip.includes(':'))
  ).length;
  
  console.log(`✅ Loaded ${conversions.length} valid conversions`);
  console.log(`🌐 IP Analysis: ${dualIPCount} dual-IP scenarios, ${ipv6Count} with IPv6`);
  
  return conversions;
}

// ENHANCED: Build date-based indexes with complete IP data
async function buildEnhancedConversionDateIndexes(redis, conversions, maxTime) {
  const indexStartTime = Date.now();
  console.log(`📅 Building ENHANCED date indexes for ${conversions.length} conversions...`);
  
  // Group conversions by date
  const dateGroups = {};
  let earliestDate = null;
  let latestDate = null;
  
  for (const conversion of conversions) {
    const date = new Date(conversion.timestamp);
    const dateKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    
    if (!dateGroups[dateKey]) {
      dateGroups[dateKey] = [];
    }
    
    dateGroups[dateKey].push(conversion);
    
    // Track date range
    if (!earliestDate || date < earliestDate) earliestDate = date;
    if (!latestDate || date > latestDate) latestDate = date;
  }
  
  console.log(`📅 Grouped into ${Object.keys(dateGroups).length} date buckets`);
  
  // Create enhanced date indexes in Redis
  let dateIndexesCreated = 0;
  const dateEntries = Object.entries(dateGroups);
  
  for (const [dateKey, dateConversions] of dateEntries) {
    if (Date.now() - indexStartTime > maxTime - 1000) {
      console.log(`⏰ Time limit reached for date indexing`);
      break;
    }
    
    try {
      const indexKey = `conversion_index_date:${dateKey}`;
      
      // Calculate enhanced daily metrics
      const dailyMetrics = calculateDailyMetrics(dateConversions);
      
      const indexData = {
        date_key: dateKey,
        conversion_count: dateConversions.length,
        conversions: dateConversions, // Store all conversions with complete IP analysis data
        created_at: new Date().toISOString(),
        
        // Enhanced metrics with detailed IP attribution analysis
        total_revenue: dailyMetrics.total_revenue,
        dual_ip_count: dailyMetrics.dual_ip_count,
        ipv6_count: dailyMetrics.ipv6_count,
        attribution_methods: dailyMetrics.attribution_methods,
        unique_primary_ips: dailyMetrics.unique_primary_ips,
        unique_conversion_ips: dailyMetrics.unique_conversion_ips,
        unique_pageview_ips: dailyMetrics.unique_pageview_ips,
        top_sources: dailyMetrics.top_sources,
        ip_extraction_methods: dailyMetrics.ip_extraction_methods,
        
        // NEW: IP attribution success analysis
        ip_attribution_breakdown: {
          primary_ip_attribution_wins: dailyMetrics.ip_attribution_success.primary_ip_wins,
          conversion_ip_attribution_wins: dailyMetrics.ip_attribution_success.conversion_ip_wins,
          pageview_ip_attribution_wins: dailyMetrics.ip_attribution_success.pageview_ip_wins,
          non_ip_attribution_wins: dailyMetrics.ip_attribution_success.non_ip_attribution,
          ip_attribution_success_rate: ((dailyMetrics.ip_attribution_success.primary_ip_wins + 
                                       dailyMetrics.ip_attribution_success.conversion_ip_wins + 
                                       dailyMetrics.ip_attribution_success.pageview_ip_wins) / 
                                       dateConversions.length * 100).toFixed(2)
        },
        
        // NEW: IP version analysis for pageview matching insights
        ip_version_insights: {
          ipv4_only_conversions: dailyMetrics.ip_version_analysis.ipv4_only_conversions,
          ipv6_only_conversions: dailyMetrics.ip_version_analysis.ipv6_only_conversions,
          dual_stack_conversions: dailyMetrics.ip_version_analysis.dual_stack_conversions,
          ipv4_attribution_success: dailyMetrics.ip_version_analysis.ipv4_attribution_success,
          ipv6_attribution_success: dailyMetrics.ip_version_analysis.ipv6_attribution_success,
          ipv4_success_rate: dailyMetrics.ip_version_analysis.ipv4_only_conversions > 0 ? 
            (dailyMetrics.ip_version_analysis.ipv4_attribution_success / dailyMetrics.ip_version_analysis.ipv4_only_conversions * 100).toFixed(2) : '0.00',
          ipv6_success_rate: dailyMetrics.ip_version_analysis.ipv6_only_conversions > 0 ? 
            (dailyMetrics.ip_version_analysis.ipv6_attribution_success / dailyMetrics.ip_version_analysis.ipv6_only_conversions * 100).toFixed(2) : '0.00'
        },
        
        // NEW: Dual-stack scenario analysis for recovery optimization
        dual_stack_analysis: {
          ipv6_primary_ipv4_conversion_scenarios: dailyMetrics.dual_stack_scenarios.ipv6_primary_ipv4_conversion,
          ipv4_primary_ipv6_conversion_scenarios: dailyMetrics.dual_stack_scenarios.ipv4_primary_ipv6_conversion,
          ipv6_wins_in_dual_stack: dailyMetrics.dual_stack_scenarios.ipv6_wins_in_dual_stack,
          ipv4_wins_in_dual_stack: dailyMetrics.dual_stack_scenarios.ipv4_wins_in_dual_stack,
          dual_stack_preference: dailyMetrics.dual_stack_scenarios.ipv4_wins_in_dual_stack > dailyMetrics.dual_stack_scenarios.ipv6_wins_in_dual_stack ? 'ipv4' : 'ipv6'
        }
      };
      
      await redis(`setex/${indexKey}/7200/${encodeURIComponent(JSON.stringify(indexData))}`); // 2 hours TTL
      dateIndexesCreated++;
      
      if (dateIndexesCreated % 10 === 0) {
        console.log(`📅 Enhanced date indexing progress: ${dateIndexesCreated}/${dateEntries.length} indexes created`);
      }
      
    } catch (dateError) {
      console.log(`⚠️ Error creating enhanced date index for ${dateKey}:`, dateError.message);
    }
  }
  
  console.log(`✅ Enhanced date indexes: ${dateIndexesCreated} created`);
  
  return {
    date_indexes_created: dateIndexesCreated,
    date_range: {
      earliest: earliestDate?.toISOString(),
      latest: latestDate?.toISOString(),
      days_covered: Object.keys(dateGroups).length
    },
    processing_time_ms: Date.now() - indexStartTime
  };
}

// NEW: Build IP-specific analytics indexes
async function buildIPAnalyticsIndexes(redis, conversions, maxTime) {
  const indexStartTime = Date.now();
  console.log(`🌐 Building IP analytics indexes...`);
  
  const ipAnalytics = {
    primary_ips: new Set(),
    conversion_ips: new Set(),
    dual_ip_conversions: [],
    ipv6_conversions: [],
    extraction_methods: {},
    attribution_by_ip_type: {}
  };
  
  // Analyze IP patterns
  for (const conversion of conversions) {
    // Track unique IPs
    if (conversion.primary_ip) ipAnalytics.primary_ips.add(conversion.primary_ip);
    if (conversion.conversion_ip) ipAnalytics.conversion_ips.add(conversion.conversion_ip);
    
    // Track dual IP scenarios
    if (conversion.dual_ip_scenario) {
      ipAnalytics.dual_ip_conversions.push({
        timestamp: conversion.timestamp,
        email: conversion.email,
        primary_ip: conversion.primary_ip,
        conversion_ip: conversion.conversion_ip,
        attribution_method: conversion.attribution_method
      });
    }
    
    // Track IPv6 usage
    const hasIPv6 = (conversion.primary_ip && conversion.primary_ip.includes(':')) || 
                   (conversion.conversion_ip && conversion.conversion_ip.includes(':')) || 
                   (conversion.pageview_ip && conversion.pageview_ip.includes(':'));
    
    if (hasIPv6) {
      ipAnalytics.ipv6_conversions.push({
        timestamp: conversion.timestamp,
        email: conversion.email,
        primary_ip: conversion.primary_ip,
        conversion_ip: conversion.conversion_ip,
        pageview_ip: conversion.pageview_ip
      });
    }
    
    // Track extraction methods
    if (conversion.ip_extraction_methods) {
      Object.entries(conversion.ip_extraction_methods).forEach(([method, count]) => {
        ipAnalytics.extraction_methods[method] = (ipAnalytics.extraction_methods[method] || 0) + 1;
      });
    }
  }
  
  // Store IP analytics index
  let ipIndexesCreated = 0;
  
  try {
    const analyticsKey = 'ip_analytics:summary';
    const analyticsData = {
      unique_primary_ips: ipAnalytics.primary_ips.size,
      unique_conversion_ips: ipAnalytics.conversion_ips.size,
      dual_ip_count: ipAnalytics.dual_ip_conversions.length,
      ipv6_count: ipAnalytics.ipv6_conversions.length,
      dual_ip_percentage: ((ipAnalytics.dual_ip_conversions.length / conversions.length) * 100).toFixed(2),
      extraction_methods: ipAnalytics.extraction_methods,
      created_at: new Date().toISOString(),
      total_conversions_analyzed: conversions.length
    };
    
    await redis(`setex/${analyticsKey}/7200/${encodeURIComponent(JSON.stringify(analyticsData))}`);
    ipIndexesCreated++;
    
    // Store detailed dual IP index if we have time
    if (Date.now() - indexStartTime < maxTime - 2000 && ipAnalytics.dual_ip_conversions.length > 0) {
      const dualIPKey = 'ip_analytics:dual_ip_details';
      await redis(`setex/${dualIPKey}/7200/${encodeURIComponent(JSON.stringify(ipAnalytics.dual_ip_conversions))}`);
      ipIndexesCreated++;
    }
    
    // Store IPv6 usage index if we have time
    if (Date.now() - indexStartTime < maxTime - 1000 && ipAnalytics.ipv6_conversions.length > 0) {
      const ipv6Key = 'ip_analytics:ipv6_usage';
      await redis(`setex/${ipv6Key}/7200/${encodeURIComponent(JSON.stringify(ipAnalytics.ipv6_conversions))}`);
      ipIndexesCreated++;
    }
    
  } catch (analyticsError) {
    console.log(`⚠️ Error creating IP analytics indexes:`, analyticsError.message);
  }
  
  console.log(`✅ IP analytics indexes: ${ipIndexesCreated} created`);
  
  return {
    ip_indexes_created: ipIndexesCreated,
    unique_primary_ips: ipAnalytics.primary_ips.size,
    unique_conversion_ips: ipAnalytics.conversion_ips.size,
    dual_ip_count: ipAnalytics.dual_ip_conversions.length,
    ipv6_count: ipAnalytics.ipv6_conversions.length,
    dual_ip_percentage: ((ipAnalytics.dual_ip_conversions.length / conversions.length) * 100).toFixed(2),
    extraction_methods: ipAnalytics.extraction_methods,
    processing_time_ms: Date.now() - indexStartTime
  };
}

// Calculate enhanced daily metrics with detailed IP attribution analysis
function calculateDailyMetrics(conversions) {
  const metrics = {
    total_revenue: 0,
    dual_ip_count: 0,
    ipv6_count: 0,
    attribution_methods: {},
    unique_primary_ips: new Set(),
    unique_conversion_ips: new Set(),
    unique_pageview_ips: new Set(),
    top_sources: {},
    ip_extraction_methods: {},
    
    // Enhanced IP attribution analysis
    ip_attribution_success: {
      primary_ip_wins: 0,
      conversion_ip_wins: 0,
      pageview_ip_wins: 0,
      non_ip_attribution: 0
    },
    ip_version_analysis: {
      ipv4_only_conversions: 0,
      ipv6_only_conversions: 0,
      dual_stack_conversions: 0,
      ipv4_attribution_success: 0,
      ipv6_attribution_success: 0
    },
    dual_stack_scenarios: {
      ipv6_primary_ipv4_conversion: 0,
      ipv4_primary_ipv6_conversion: 0,
      ipv6_wins_in_dual_stack: 0,
      ipv4_wins_in_dual_stack: 0
    }
  };
  
  for (const conversion of conversions) {
    // Revenue
    metrics.total_revenue += parseFloat(conversion.order_total) || 0;
    
    // Basic IP scenarios
    if (conversion.dual_ip_scenario) metrics.dual_ip_count++;
    
    const hasIPv6 = (conversion.primary_ip && conversion.primary_ip.includes(':')) || 
                   (conversion.conversion_ip && conversion.conversion_ip.includes(':'));
    if (hasIPv6) metrics.ipv6_count++;
    
    // Attribution methods
    const method = conversion.attribution_method || 'none';
    metrics.attribution_methods[method] = (metrics.attribution_methods[method] || 0) + 1;
    
    // Unique IPs tracking
    if (conversion.primary_ip) metrics.unique_primary_ips.add(conversion.primary_ip);
    if (conversion.conversion_ip) metrics.unique_conversion_ips.add(conversion.conversion_ip);
    if (conversion.pageview_ip) metrics.unique_pageview_ips.add(conversion.pageview_ip);
    
    // Sources
    const source = conversion.source || 'unknown';
    metrics.top_sources[source] = (metrics.top_sources[source] || 0) + 1;
    
    // IP extraction methods
    if (conversion.ip_extraction_methods) {
      Object.entries(conversion.ip_extraction_methods).forEach(([method, type]) => {
        metrics.ip_extraction_methods[type] = (metrics.ip_extraction_methods[type] || 0) + 1;
      });
    }
    
    // ENHANCED: IP attribution success analysis
    if (method === 'primary_ip_match') {
      metrics.ip_attribution_success.primary_ip_wins++;
    } else if (method === 'conversion_ip_match') {
      metrics.ip_attribution_success.conversion_ip_wins++;
    } else if (method === 'pageview_ip_match') {
      metrics.ip_attribution_success.pageview_ip_wins++;
    } else {
      metrics.ip_attribution_success.non_ip_attribution++;
    }
    
    // IP version analysis
    const ipVersionMix = conversion.ip_version_mix || 'unknown';
    if (ipVersionMix === 'ipv4_only') {
      metrics.ip_version_analysis.ipv4_only_conversions++;
    } else if (ipVersionMix === 'ipv6_only') {
      metrics.ip_version_analysis.ipv6_only_conversions++;
    } else if (ipVersionMix === 'dual_stack') {
      metrics.ip_version_analysis.dual_stack_conversions++;
    }
    
    // Track attribution success by IP version
    const winningIPType = conversion.winning_ip_type;
    if (winningIPType === 'ipv4') {
      metrics.ip_version_analysis.ipv4_attribution_success++;
    } else if (winningIPType === 'ipv6') {
      metrics.ip_version_analysis.ipv6_attribution_success++;
    }
    
    // Dual-stack scenario analysis
    if (conversion.dual_ip_scenario) {
      const primaryType = getIPType(conversion.primary_ip);
      const conversionType = getIPType(conversion.conversion_ip);
      
      if (primaryType === 'ipv6' && conversionType === 'ipv4') {
        metrics.dual_stack_scenarios.ipv6_primary_ipv4_conversion++;
        if (winningIPType === 'ipv6') metrics.dual_stack_scenarios.ipv6_wins_in_dual_stack++;
        if (winningIPType === 'ipv4') metrics.dual_stack_scenarios.ipv4_wins_in_dual_stack++;
      } else if (primaryType === 'ipv4' && conversionType === 'ipv6') {
        metrics.dual_stack_scenarios.ipv4_primary_ipv6_conversion++;
        if (winningIPType === 'ipv4') metrics.dual_stack_scenarios.ipv4_wins_in_dual_stack++;
        if (winningIPType === 'ipv6') metrics.dual_stack_scenarios.ipv6_wins_in_dual_stack++;
      }
    }
  }
  
  // Convert Sets to counts
  metrics.unique_primary_ips = metrics.unique_primary_ips.size;
  metrics.unique_conversion_ips = metrics.unique_conversion_ips.size;
  metrics.unique_pageview_ips = metrics.unique_pageview_ips.size;
  
  return metrics;
}

// Helper functions for IP attribution analysis
function getWinningIPValue(conversion) {
  // Determine which actual IP address value succeeded in attribution
  const method = conversion.attribution_method;
  
  if (method === 'primary_ip_match') return conversion.primary_ip;
  if (method === 'conversion_ip_match') return conversion.conversion_ip;
  if (method === 'pageview_ip_match') return conversion.pageview_ip;
  
  // For other methods (session_id, device_signature), return the main IP used
  return conversion.ip_address || conversion.conversion_ip || conversion.primary_ip;
}

function getIPType(ipAddress) {
  if (!ipAddress) return 'unknown';
  return ipAddress.includes(':') ? 'ipv6' : 'ipv4';
}

function analyzeIPVersionMix(conversion) {
  const ips = [conversion.primary_ip, conversion.conversion_ip, conversion.pageview_ip].filter(Boolean);
  const hasIPv4 = ips.some(ip => !ip.includes(':'));
  const hasIPv6 = ips.some(ip => ip.includes(':'));
  
  if (hasIPv4 && hasIPv6) return 'dual_stack';
  if (hasIPv6) return 'ipv6_only';
  if (hasIPv4) return 'ipv4_only';
  return 'unknown';
}

function determinePageviewLookupPriority(conversion) {
  // Return the order IPs should be tried for pageview attribution lookup
  const priorities = [];
  
  if (conversion.primary_ip) {
    priorities.push({
      ip: conversion.primary_ip,
      type: 'primary_ip',
      ip_version: getIPType(conversion.primary_ip),
      extraction_method: conversion.ip_extraction_methods?.PIP || 'unknown'
    });
  }
  
  if (conversion.conversion_ip && conversion.conversion_ip !== conversion.primary_ip) {
    priorities.push({
      ip: conversion.conversion_ip,
      type: 'conversion_ip', 
      ip_version: getIPType(conversion.conversion_ip),
      extraction_method: conversion.ip_extraction_methods?.CIP || 'unknown'
    });
  }
  
  if (conversion.pageview_ip && 
      conversion.pageview_ip !== conversion.primary_ip && 
      conversion.pageview_ip !== conversion.conversion_ip) {
    priorities.push({
      ip: conversion.pageview_ip,
      type: 'pageview_ip',
      ip_version: getIPType(conversion.pageview_ip),
      extraction_method: conversion.ip_extraction_methods?.IP || 'unknown'
    });
  }
  
  return priorities;
}

function generatePotentialPageviewKeys(conversion) {
  // Generate all possible Redis keys that could contain pageview attribution for this conversion
  const keys = [];
  const ips = [conversion.primary_ip, conversion.conversion_ip, conversion.pageview_ip].filter(Boolean);
  
  // Remove duplicates and encode for Redis
  const uniqueIPs = [...new Set(ips)];
  
  for (const ip of uniqueIPs) {
    const encodedIP = ip.replace(/:/g, '_'); // IPv6-safe encoding
    keys.push({
      redis_key: `attribution_ip_${encodedIP}`,
      original_ip: ip,
      ip_version: getIPType(ip),
      ip_type: getIPTypeFromConversion(conversion, ip)
    });
  }
  
  return keys;
}

function getIPTypeFromConversion(conversion, targetIP) {
  // Determine if this IP is primary, conversion, or pageview
  if (conversion.primary_ip === targetIP) return 'primary_ip';
  if (conversion.conversion_ip === targetIP) return 'conversion_ip';  
  if (conversion.pageview_ip === targetIP) return 'pageview_ip';
  return 'unknown';
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
