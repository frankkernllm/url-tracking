// File: netlify/functions/attribution-recovery-june-analysis.js
// COMPLETELY REWRITTEN - Safe results object construction

const handler = async (event, context) => {
  console.log('🚀 Starting June 23-30 conversion analysis...');
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;
  
  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  try {
    const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
    const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    const redis = async (command) => {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` }
      });
      return response.json();
    };

    // Initialize results object early to avoid undefined errors
    let results = {
      total_conversions_analyzed: 0,
      currently_attributed: 0,
      currently_unattributed: 0,
      attribution_rate: 0,
      recovery_successful: 0,
      sample_conversions: [],
      redis_keys_found: 0,
      date_range: {
        start: '2025-06-23T00:00:00.000Z',
        end: '2025-06-30T23:59:59.999Z'
      },
      analysis_summary: {},
      method_breakdown: {},
      attribution_improvements: [],
      debug_samples: []
    };

    // Date range
    const startDate = new Date('2025-06-23T00:00:00.000Z');
    const endDate = new Date('2025-06-30T23:59:59.999Z');
    const startTimestamp = startDate.getTime();
    const endTimestamp = endDate.getTime();

    console.log(`📅 Analyzing conversions from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    // Get conversion keys using CORRECT pattern: conversions:*
    console.log('🔍 Scanning for conversions using CORRECT pattern: conversions:*');
    
    let allKeys = [];
    
    try {
      let cursor = '0';
      let iterations = 0;
      
      do {
        const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
        if (result.result && result.result[1]) {
          cursor = result.result[0];
          const keys = result.result[1];
          allKeys = allKeys.concat(keys);
          iterations++;
          
          console.log(`✅ Batch ${iterations}: Found ${keys.length} conversion keys (total: ${allKeys.length}, cursor: ${cursor})`);
          
          if (allKeys.length > 10000) {
            console.warn('⚠️ Breaking after 10,000 keys for memory safety');
            break;
          }
          
          if (iterations % 10 === 0) {
            await new Promise(resolve => setTimeout(resolve, 50));
          }
        } else {
          console.log(`⚠️ No results from SCAN at cursor ${cursor}, stopping iteration`);
          break;
        }
      } while (cursor !== '0');
      
      console.log(`🎯 SCAN COMPLETE: ${allKeys.length} conversion keys found using pattern 'conversions:*'`);
      results.redis_keys_found = allKeys.length;
      
    } catch (error) {
      console.error('❌ SCAN failed:', error.message);
    }

    // Process conversions for June 23-30 analysis WITH FULL RECOVERY LOGIC
    let totalConversions = 0;
    let currentlyAttributed = 0;
    let recoverySuccessful = 0;
    let sampleConversions = [];
    const attributionImprovements = [];
    const methodBreakdown = {};
    const debugSamples = []; // Store debug info for response
    
    console.log('🔍 Processing conversions for date range analysis WITH RECOVERY...');
    
    // Attribution priority system (same as track.js)
    const attributionPriorities = [
      { name: 'session_id_match', field: 'SSID', keyPrefix: 'attribution_session_', points: 300 },
      { name: 'primary_ip_match', field: 'PIP', keyPrefix: 'attribution_ip_', points: 280 },
      { name: 'conversion_ip_match', field: 'CIP', keyPrefix: 'attribution_ip_', points: 260 },
      { name: 'pageview_ip_match', field: 'IP', keyPrefix: 'attribution_ip_', points: 240 },
      { name: 'device_signature_match', field: 'dsig', keyPrefix: 'attribution_fp_', points: 220 },
      { name: 'screen_hash_match', field: 'SVV', keyPrefix: 'attribution_screen_', points: 200 },
      { name: 'webgl_match', field: 'gsig', keyPrefix: 'attribution_webgl_', points: 180 }
    ];

    // Helper function to extract conversion parameters
    function extractConversionParameters(conversion) {
      return {
        email: conversion.email || 'unknown',
        
        // IP field names (corrected)
        PIP: conversion.primary_ip || null,
        CIP: conversion.conversion_ip || null, 
        IP: conversion.pageview_ip || conversion.primary_ip || null,
        
        // Attribution parameter field names (corrected)
        SSID: conversion.ssid || conversion.session_id || null,
        dsig: conversion.dsig || conversion.device_signature || null,
        SVV: conversion.SVV || conversion.SVVV || conversion.screen_value || null,
        gsig: conversion.gsig || conversion.gpu_signature || null,
        
        // Current attribution status
        current_attribution_found: conversion.attribution_found || false,
        current_attribution_method: conversion.attribution_method || 'none',
        current_attribution_score: conversion.attribution_score || 0,
        current_source: conversion.source || 'direct',
        current_landing_page: conversion.landing_page || 'none',
        
        // Metadata
        timestamp: conversion.timestamp,
        order_total: conversion.order_total || 0
      };
    }
    
    for (const key of allKeys) {
      try {
        const conversionResult = await redis(`get/${key}`);
        const conversionData = conversionResult.result;
        if (!conversionData) continue;
        
        let conversion;
        try {
          conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
        } catch (parseError) {
          console.log(`⚠️ Failed to parse conversion data from ${key}`);
          continue;
        }
        
        if (!conversion.timestamp) continue;
        
        const conversionTimestamp = new Date(conversion.timestamp).getTime();
        
        // Check if conversion is in our date range
        if (conversionTimestamp >= startTimestamp && conversionTimestamp <= endTimestamp) {
          totalConversions++;
          
          // Extract conversion parameters for analysis
          const params = extractConversionParameters(conversion);
          
          console.log(`📧 CONVERSION ${totalConversions}/108: ${params.email}`);
          console.log(`📅 Date: ${params.timestamp}`);
          console.log(`📊 Current Status: ${params.current_attribution_found ? '✅ ATTRIBUTED' : '❌ UNATTRIBUTED'}`);
          
          // 🔧 DEBUG: Show raw conversion data structure
          if (totalConversions <= 3) {
            const debugInfo = {
              conversion_number: totalConversions,
              email: params.email,
              raw_fields: Object.keys(conversion),
              raw_values: {
                email: conversion.email,
                timestamp: conversion.timestamp,
                attribution_found: conversion.attribution_found,
                session_id: conversion.session_id || 'MISSING',
                ssid: conversion.ssid || 'MISSING',
                dsig: conversion.dsig || 'MISSING',
                device_signature: conversion.device_signature || 'MISSING',
                primary_ip: conversion.primary_ip || 'MISSING',
                SVV: conversion.SVV || 'MISSING',
                SVVV: conversion.SVVV || 'MISSING'
              },
              extracted_params: {
                SSID: params.SSID || 'MISSING',
                dsig: params.dsig || 'MISSING',
                PIP: params.PIP || 'MISSING',
                CIP: params.CIP || 'MISSING',
                SVV: params.SVV || 'MISSING'
              }
            };
            debugSamples.push(debugInfo);
            console.log(`🔍 DEBUG - Conversion ${totalConversions}:`, debugInfo);
          }
          
          if (params.current_attribution_found) {
            console.log(`🎯 Current Method: ${params.current_attribution_method} (${params.current_attribution_score} points)`);
            currentlyAttributed++;
          }

          // Show available data for debugging
          const availableData = {
            SSID: !!params.SSID,
            PIP: !!params.PIP,
            CIP: !!params.CIP,
            IP: !!params.IP,
            dsig: !!params.dsig,
            SVV: !!params.SVV,
            gsig: !!params.gsig
          };
          console.log(`🔍 Available Data:`, availableData);

          // 🔧 RECOVERY LOGIC: Try attribution methods in priority order
          let bestAttribution = null;
          let bestScore = params.current_attribution_score;

          // Only try recovery for unattributed conversions or first few for debugging
          if (!params.current_attribution_found || totalConversions <= 3) {
            console.log(`🔍 Attempting recovery for: ${params.email}`);
            
            // Store lookup attempts for debug
            const lookupAttempts = [];
            
            for (const priority of attributionPriorities) {
              const fieldValue = params[priority.field];
              if (!fieldValue) {
                console.log(`⚠️ ${priority.name}: No ${priority.field} data available`);
                lookupAttempts.push({
                  method: priority.name,
                  field: priority.field,
                  value: 'MISSING',
                  result: 'SKIPPED'
                });
                continue;
              }

              // Create lookup key
              let lookupKey;
              if (priority.keyPrefix === 'attribution_ip_') {
                lookupKey = `${priority.keyPrefix}${fieldValue.replace(/\./g, '_').replace(/:/g, '_')}`;
              } else {
                lookupKey = `${priority.keyPrefix}${fieldValue}`;
              }

              console.log(`🌐 ${priority.name}: Trying ${priority.field}: ${fieldValue}`);
              console.log(`🔑 Looking up key: ${lookupKey}`);

              try {
                const attributionResult = await redis(`get/${lookupKey}`);
                const attributionData = attributionResult.result;
                
                if (attributionData) {
                  const parsed = JSON.parse(attributionData);
                  console.log(`✅ ATTRIBUTION FOUND: ${priority.name} (${priority.points} points)`);
                  console.log(`📋 Attribution data:`, {
                    source: parsed.source,
                    landing_page: parsed.landing_page?.substring(0, 50),
                    utm_source: parsed.utm_source
                  });
                  
                  lookupAttempts.push({
                    method: priority.name,
                    field: priority.field,
                    value: fieldValue,
                    lookup_key: lookupKey,
                    result: 'FOUND',
                    data: {
                      source: parsed.source,
                      landing_page: parsed.landing_page?.substring(0, 50)
                    }
                  });
                  
                  if (priority.points > bestScore) {
                    bestAttribution = {
                      method: priority.name,
                      score: priority.points,
                      data: parsed,
                      lookup_key: lookupKey
                    };
                    bestScore = priority.points;
                  }
                } else {
                  console.log(`❌ No attribution data found for key: ${lookupKey}`);
                  lookupAttempts.push({
                    method: priority.name,
                    field: priority.field,
                    value: fieldValue,
                    lookup_key: lookupKey,
                    result: 'NOT_FOUND'
                  });
                }
              } catch (error) {
                console.log(`❌ Error looking up ${lookupKey}:`, error.message);
                lookupAttempts.push({
                  method: priority.name,
                  field: priority.field,
                  value: fieldValue,
                  lookup_key: lookupKey,
                  result: 'ERROR',
                  error: error.message
                });
              }
            }
            
            // Add lookup attempts to debug info for first few conversions
            if (totalConversions <= 3 && debugSamples.length > 0) {
              debugSamples[debugSamples.length - 1].lookup_attempts = lookupAttempts;
            }
          }

          // Determine improvement type
          if (bestAttribution) {
            let improvementType = '';
            
            if (!params.current_attribution_found) {
              improvementType = 'NEW_ATTRIBUTION';
              recoverySuccessful++;
            } else if (bestScore > params.current_attribution_score) {
              improvementType = 'BETTER_ATTRIBUTION';
              recoverySuccessful++;
            } else {
              improvementType = 'ALTERNATIVE_FOUND'; // Not actually an improvement
            }

            // Only count as improvement if it's actually better
            if (improvementType === 'NEW_ATTRIBUTION' || improvementType === 'BETTER_ATTRIBUTION') {
              // Track method breakdown
              methodBreakdown[bestAttribution.method] = (methodBreakdown[bestAttribution.method] || 0) + 1;

              attributionImprovements.push({
                conversion_key: key,
                email: params.email,
                timestamp: params.timestamp,
                improvement_type: improvementType,
                score_improvement: bestScore - params.current_attribution_score,
                previous_status: {
                  attribution_found: params.current_attribution_found,
                  attribution_method: params.current_attribution_method,
                  attribution_score: params.current_attribution_score,
                  source: params.current_source,
                  landing_page: params.current_landing_page
                },
                new_attribution: {
                  attribution_method: bestAttribution.method,
                  attribution_score: bestAttribution.score,
                  source: bestAttribution.data.source || 'direct',
                  landing_page: bestAttribution.data.landing_page || 'none',
                  utm_source: bestAttribution.data.utm_source || null,
                  utm_campaign: bestAttribution.data.utm_campaign || null,
                  lookup_key_used: bestAttribution.lookup_key
                }
              });

              console.log(`📈 Improvement: ${improvementType} (+${bestScore - params.current_attribution_score} points)`);
            }
          } else {
            console.log(`⚠️ No attribution found for this conversion`);
          }
          
          // Store sample for debugging
          if (sampleConversions.length < 5) {
            sampleConversions.push(key.substring(0, 50));
          }
          
          console.log('---');
        }
      } catch (error) {
        console.log(`⚠️ Error processing conversion ${key}:`, error.message);
      }
    }
    
    // Update results object with recovery analysis
    const analysisStats = {
      attribution_rate_before: totalConversions > 0 ? Math.round((currentlyAttributed / totalConversions) * 100) : 0,
      potential_attribution_rate: totalConversions > 0 ? Math.round(((currentlyAttributed + recoverySuccessful) / totalConversions) * 100) : 0,
      improvements_possible: recoverySuccessful,
      improvement_percentage: totalConversions > 0 ? Math.round((recoverySuccessful / totalConversions) * 100) : 0
    };

    results.total_conversions_analyzed = totalConversions;
    results.currently_attributed = currentlyAttributed;
    results.currently_unattributed = totalConversions - currentlyAttributed;
    results.attribution_rate = analysisStats.attribution_rate_before;
    results.recovery_successful = recoverySuccessful;
    results.sample_conversions = sampleConversions;
    results.analysis_summary = analysisStats;
    results.method_breakdown = methodBreakdown;
    results.attribution_improvements = attributionImprovements;
    results.debug_samples = debugSamples; // Include debug info in response

    console.log('📊 FINAL RECOVERY ANALYSIS RESULTS:');
    console.log(`Total Conversions: ${results.total_conversions_analyzed}`);
    console.log(`Currently Attributed: ${results.currently_attributed} (${results.attribution_rate}%)`);
    console.log(`Recovery Successful: ${results.recovery_successful} conversions`);
    console.log(`Potential Attribution Rate: ${analysisStats.potential_attribution_rate}%`);
    console.log(`Improvement Methods:`, methodBreakdown);
    console.log(`Debug Samples:`, debugSamples);
    console.log(`Redis Keys Found: ${results.redis_keys_found}`);
    console.log(`Sample Keys: ${results.sample_conversions.join(', ')}`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'June 23-30 conversion recovery analysis completed',
        results: results
      })
    };

  } catch (error) {
    console.error('❌ Analysis error:', error);
    
    // Return safe fallback results even on error
    const fallbackResults = {
      total_conversions_analyzed: 0,
      currently_attributed: 0,
      currently_unattributed: 0,
      attribution_rate: 0,
      recovery_successful: 0,
      error_message: error.message
    };
    
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        success: false,
        error: 'Analysis failed', 
        details: error.message,
        results: fallbackResults
      })
    };
  }
};

module.exports = { handler };
