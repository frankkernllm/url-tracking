// File: netlify/functions/attribution-recovery-june-analysis.js
// FIXED VERSION with correct field mapping and comprehensive analysis
// 🔧 UPDATED: Corrected all field names to match actual track.js storage

const handler = async (event, context) => {
  console.log('🚀 Starting June 23-30 conversion analysis...');
  
  // Handle CORS
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  // Authentication
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== 'ojoy_track_2025_secure_key_v1') {
    console.log('❌ Invalid or missing API key');
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid or missing API key' })
    };
  }

  try {
    const Redis = require('ioredis');
    const redis = new Redis(process.env.REDIS_URL);

    // Define date range for June 23-30, 2025
    const startDate = new Date('2025-06-23T00:00:00.000Z');
    const endDate = new Date('2025-06-30T23:59:59.999Z');
    const startTimestamp = startDate.getTime();
    const endTimestamp = endDate.getTime();

    console.log(`📅 Analyzing conversions from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    // Get all conversion keys
    const allKeys = await redis.keys('conversion_*');
    console.log(`🔍 Found ${allKeys.length} total conversion keys in Redis`);

    // Filter conversions within date range
    const conversionsInRange = [];
    let totalConversions = 0;
    
    for (const key of allKeys) {
      try {
        const conversionData = await redis.get(key);
        if (!conversionData) continue;
        
        const conversion = JSON.parse(conversionData);
        const conversionTimestamp = new Date(conversion.timestamp).getTime();
        
        if (conversionTimestamp >= startTimestamp && conversionTimestamp <= endTimestamp) {
          conversionsInRange.push({ key, data: conversion });
          totalConversions++;
        }
      } catch (error) {
        console.log(`⚠️ Error parsing conversion ${key}:`, error.message);
      }
    }

    console.log(`✅ Found ${totalConversions} conversions in June 23-30 date range`);

    // 🔧 FIXED: Corrected field mapping to match actual track.js storage
    function extractConversionParameters(conversion) {
      return {
        email: conversion.email || 'unknown',
        
        // 🔧 FIXED: Correct IP field names
        PIP: conversion.primary_ip || null,
        CIP: conversion.conversion_ip || null, 
        IP: conversion.pageview_ip || conversion.primary_ip || null,
        
        // 🔧 FIXED: Correct attribution parameter field names
        SSID: conversion.ssid || conversion.session_id || null,           // Fixed: Check ssid first
        dsig: conversion.dsig || conversion.device_signature || null,    // Fixed: Check dsig first
        SVV: conversion.SVV || conversion.SVVV || conversion.screen_value || null,  // Fixed: Check SVVV
        gsig: conversion.gsig || conversion.gpu_signature || null,       // Fixed: Check gsig first
        
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

    // Process each conversion
    let currentlyAttributed = 0;
    let currentlyUnattributed = 0;
    let recoverySuccessful = 0;
    const attributionImprovements = [];
    const methodBreakdown = {};

    for (let i = 0; i < conversionsInRange.length; i++) {
      const { key, data: conversion } = conversionsInRange[i];
      const params = extractConversionParameters(conversion);
      
      console.log(`📧 CONVERSION ${i + 1}/${totalConversions}: ${params.email}`);
      console.log(`📅 Date: ${params.timestamp}`);
      console.log(`📊 Current Status: ${params.current_attribution_found ? '✅ ATTRIBUTED' : '❌ UNATTRIBUTED'}`);
      
      if (params.current_attribution_found) {
        console.log(`🎯 Current Method: ${params.current_attribution_method} (${params.current_attribution_score} points)`);
        currentlyAttributed++;
      } else {
        currentlyUnattributed++;
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

      // Try attribution methods in priority order
      let bestAttribution = null;
      let bestScore = params.current_attribution_score;

      for (const priority of attributionPriorities) {
        const fieldValue = params[priority.field];
        if (!fieldValue) continue;

        // Create lookup key
        let lookupKey;
        if (priority.keyPrefix === 'attribution_ip_') {
          lookupKey = `${priority.keyPrefix}${fieldValue.replace(/\./g, '_').replace(/:/g, '_')}`;
        } else {
          lookupKey = `${priority.keyPrefix}${fieldValue}`;
        }

        console.log(`🌐 ${priority.name}: Trying ${priority.field}: ${fieldValue}`);

        try {
          const attributionData = await redis.get(lookupKey);
          if (attributionData) {
            const parsed = JSON.parse(attributionData);
            console.log(`✅ ATTRIBUTION FOUND: ${priority.name} (${priority.points} points)`);
            
            if (priority.points > bestScore) {
              bestAttribution = {
                method: priority.name,
                score: priority.points,
                data: parsed,
                lookup_key: lookupKey
              };
              bestScore = priority.points;
            }
          }
        } catch (error) {
          console.log(`❌ Error looking up ${lookupKey}:`, error.message);
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

      console.log('---');
    }

    // Calculate statistics
    const analysisStats = {
      attribution_rate_before: Math.round((currentlyAttributed / totalConversions) * 100),
      potential_attribution_rate: Math.round(((currentlyAttributed + recoverySuccessful) / totalConversions) * 100),
      improvements_possible: recoverySuccessful,
      improvement_percentage: Math.round((recoverySuccessful / totalConversions) * 100)
    };

    const results = {
      total_conversions_analyzed: totalConversions,
      currently_attributed: currentlyAttributed,
      currently_unattributed: currentlyUnattributed,
      recovery_attempted: totalConversions,
      recovery_successful: recoverySuccessful,
      analysis_summary: analysisStats,
      method_breakdown: methodBreakdown,
      attribution_improvements: attributionImprovements
    };

    console.log('📊 FINAL ANALYSIS RESULTS:');
    console.log(`Total Conversions: ${totalConversions}`);
    console.log(`Currently Attributed: ${currentlyAttributed} (${analysisStats.attribution_rate_before}%)`);
    console.log(`Recovery Possible: ${recoverySuccessful} (${analysisStats.improvement_percentage}%)`);
    console.log(`Potential Attribution Rate: ${analysisStats.potential_attribution_rate}%`);

    redis.disconnect();

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'June 23-30 conversion analysis completed',
        results: results
      })
    };

  } catch (error) {
    console.error('❌ Analysis error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Analysis failed', 
        details: error.message 
      })
    };
  }
};

module.exports = { handler };
