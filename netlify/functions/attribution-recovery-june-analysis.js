// File: netlify/functions/attribution-recovery-june-analysis.js
// FIXED VERSION: Corrected field mapping + Process ALL conversions from June 23-30
// Compatible with actual track.js data structure

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
};

const handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  // Security check
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (!apiKey || apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  const ipinfoToken = process.env.IPINFO_TOKEN;

  const redis = async (command, timeoutMs = 5000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` },
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };

  // IPv6-safe key encoding (matching other scripts)
  function encodeIPForKey(ip) {
    return ip.replace(/:/g, '_');
  }

  // 🔧 FIXED: Extract conversion parameters from ACTUAL track.js data structure
  function extractConversionParameters(conversion) {
    const conversionAge = Math.floor((Date.now() - new Date(conversion.timestamp)) / (1000 * 60 * 60 * 24));
    
    return {
      email: conversion.email,
      timestamp: conversion.timestamp,
      
      // 🔧 FIXED: Use actual field names from track.js storage
      SSID: conversion.session_id,                               // track.js stores as 'session_id'
      
      // 🔧 FIXED: IP extraction using correct field names
      PIP: conversion.primary_ip,                                // ✅ Actual field name
      CIP: conversion.conversion_ip,                             // ✅ Actual field name  
      IP: conversion.pageview_ip || conversion.ip_address,       // ✅ Fallback to main IP
      
      // 🔧 FIXED: Device signatures - track.js doesn't store these as top-level fields
      // We'll need to extract from attribution_fields_present or use other methods
      dsig: null, // Device signatures not available in conversion records
      SVV: null,  // Screen values not available in conversion records
      gsig: null, // GPU signatures not available in conversion records
      
      // Standard attribution fields (these should exist)
      landing_page: conversion.landing_page,
      utm_source: conversion.utm_source || conversion.source,
      utm_campaign: conversion.utm_campaign || conversion.campaign,
      utm_medium: conversion.utm_medium,
      utm_content: conversion.utm_content,
      utm_term: conversion.utm_term,
      
      // Current attribution status
      current_attribution_found: conversion.attribution_found,
      current_attribution_method: conversion.attribution_method,
      current_attribution_score: conversion.attribution_score || 0,
      
      // Metadata
      conversion_age_days: conversionAge,
      requires_recovery: !conversion.attribution_found || conversion.attribution_method === 'none'
    };
  }

  // 🔧 FIXED: Enhanced attribution using 8-tier system with corrected field mapping
  async function findEnhancedAttribution(conversionData, cacheStats) {
    console.log(`🔍 Attempting attribution recovery for: ${conversionData.email}`);
    
    // Priority 1: Session ID Match (300 points)
    if (conversionData.SSID) {
      try {
        console.log('   🎯 Priority 1: Trying Session ID match:', conversionData.SSID);
        const sessionKey = `attribution_session_${conversionData.SSID}`;
        const sessionResult = await redis(`get/${sessionKey}`, 3000);
        
        if (sessionResult.result) {
          const attributionResult = await redis(`get/${sessionResult.result}`, 3000);
          if (attributionResult.result) {
            const attrData = JSON.parse(decodeURIComponent(attributionResult.result));
            console.log('   ✅ Priority 1: Session ID match found');
            return {
              method: 'session_id_match',
              score: 300,
              matched_session: conversionData.SSID,
              landing_page: attrData.landing_page,
              ...attrData
            };
          }
        }
        console.log('   ⚠️ Priority 1: Session ID lookup failed');
      } catch (error) {
        console.log('   ❌ Priority 1: Session ID error:', error.message);
      }
    }

    // Priority 2-4: IP Address Matches (280-240 points)
    const ipAddressesToTry = [
      { ip: conversionData.PIP, method: 'primary_ip_match', score: 280, label: 'Primary IP' },
      { ip: conversionData.CIP, method: 'conversion_ip_match', score: 260, label: 'Conversion IP' },
      { ip: conversionData.IP, method: 'pageview_ip_match', score: 240, label: 'Pageview IP' }
    ];

    for (const ipData of ipAddressesToTry) {
      if (ipData.ip && ipData.ip !== 'unknown') {
        try {
          console.log(`   🌐 Priority ${ipData.score === 280 ? '2' : ipData.score === 260 ? '3' : '4'}: Trying ${ipData.label}:`, ipData.ip);
          const ipKey = `attribution_ip_${encodeIPForKey(ipData.ip)}`;
          const ipResult = await redis(`get/${ipKey}`, 3000);
          
          if (ipResult.result) {
            const attributionResult = await redis(`get/${ipResult.result}`, 3000);
            if (attributionResult.result) {
              const attrData = JSON.parse(decodeURIComponent(attributionResult.result));
              console.log(`   ✅ Priority ${ipData.score === 280 ? '2' : ipData.score === 260 ? '3' : '4'}: ${ipData.label} match found`);
              return {
                method: ipData.method,
                score: ipData.score,
                matched_ip: ipData.ip,
                landing_page: attrData.landing_page,
                ...attrData
              };
            }
          }
        } catch (error) {
          console.log(`   ❌ ${ipData.label} error:`, error.message);
        }
      }
    }

    // Priority 8: Geographic Correlation (60-100 points) - if we have IP and API token
    if (conversionData.IP && ipinfoToken) {
      try {
        console.log('   🗺️ Priority 8: Attempting geographic correlation for:', conversionData.IP);
        // Implement geographic correlation here if needed
        // For now, skipping to keep script focused on main attribution methods
      } catch (error) {
        console.log('   ❌ Geographic correlation error:', error.message);
      }
    }

    console.log('   ❌ No attribution found through any method');
    return null;
  }

  // 🔧 FIXED: Process conversions with date filtering for June 23-30
  async function processConversionsJuneAnalysis() {
    console.log('🚀 Starting June 23-30 conversion analysis...');
    
    // Date range: June 23-30, 2025
    const startDate = new Date('2025-06-23T00:00:00.000Z');
    const endDate = new Date('2025-06-30T23:59:59.999Z');
    const startTimestamp = startDate.getTime();
    const endTimestamp = endDate.getTime();
    
    console.log(`📅 Analyzing conversions from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    // Get all conversion keys
    const conversionKeysResult = await redis('keys/conversions:*', 10000);
    const allConversionKeys = conversionKeysResult.result || [];
    
    console.log(`🔍 Found ${allConversionKeys.length} total conversion keys in Redis`);

    // Filter conversions by date and fetch data
    const conversionsInRange = [];
    let processedKeys = 0;
    
    for (const key of allConversionKeys) {
      try {
        const conversionResult = await redis(`get/${key}`, 3000);
        if (conversionResult.result) {
          const conversion = JSON.parse(decodeURIComponent(conversionResult.result));
          const conversionTime = new Date(conversion.timestamp).getTime();
          
          // Check if conversion is in our date range
          if (conversionTime >= startTimestamp && conversionTime <= endTimestamp) {
            conversionsInRange.push({
              ...conversion,
              redis_key: key
            });
          }
        }
        processedKeys++;
        
        // Progress logging every 100 keys
        if (processedKeys % 100 === 0) {
          console.log(`📊 Processed ${processedKeys}/${allConversionKeys.length} keys...`);
        }
      } catch (error) {
        console.log(`⚠️ Error processing key ${key}:`, error.message);
      }
    }

    console.log(`✅ Found ${conversionsInRange.length} conversions in June 23-30 date range`);

    // Analysis results
    const results = {
      total_conversions_analyzed: conversionsInRange.length,
      currently_attributed: 0,
      currently_unattributed: 0,
      recovery_attempted: 0,
      recovery_successful: 0,
      attribution_improvements: [],
      method_breakdown: {},
      date_range: {
        start: startDate.toISOString(),
        end: endDate.toISOString(),
        days_analyzed: 8
      }
    };

    const cacheStats = { hits: 0, misses: 0, api_calls: 0 };

    // Process each conversion
    for (let i = 0; i < conversionsInRange.length; i++) {
      const conversion = conversionsInRange[i];
      
      console.log(`\n📧 CONVERSION ${i + 1}/${conversionsInRange.length}: ${conversion.email}`);
      console.log(`   📅 Date: ${conversion.timestamp}`);
      console.log(`   📊 Current Status: ${conversion.attribution_found ? '✅ ATTRIBUTED' : '❌ UNATTRIBUTED'}`);
      
      if (conversion.attribution_found) {
        results.currently_attributed++;
        console.log(`   🎯 Current Method: ${conversion.attribution_method} (${conversion.attribution_score} points)`);
      } else {
        results.currently_unattributed++;
      }

      // Extract conversion parameters using fixed field mapping
      const conversionData = extractConversionParameters(conversion);
      
      console.log(`   🔍 Available Data: SSID=${!!conversionData.SSID}, PIP=${!!conversionData.PIP}, CIP=${!!conversionData.CIP}, IP=${!!conversionData.IP}`);

      // Attempt attribution recovery for ALL conversions (not just unattributed)
      results.recovery_attempted++;
      const attributionResult = await findEnhancedAttribution(conversionData, cacheStats);
      
      if (attributionResult) {
        results.recovery_successful++;
        
        // Track method breakdown
        const method = attributionResult.method;
        results.method_breakdown[method] = (results.method_breakdown[method] || 0) + 1;
        
        const improvement = {
          email: conversion.email,
          timestamp: conversion.timestamp,
          redis_key: conversion.redis_key,
          previous_status: {
            attribution_found: conversion.attribution_found,
            attribution_method: conversion.attribution_method || 'none',
            attribution_score: conversion.attribution_score || 0,
            landing_page: conversion.landing_page || null
          },
          new_attribution: {
            attribution_found: true,
            attribution_method: attributionResult.method,
            attribution_score: attributionResult.score,
            landing_page: attributionResult.landing_page,
            source: attributionResult.source,
            utm_campaign: attributionResult.utm_campaign
          },
          improvement_type: conversion.attribution_found ? 'BETTER_ATTRIBUTION' : 'NEW_ATTRIBUTION',
          score_improvement: attributionResult.score - (conversion.attribution_score || 0)
        };
        
        results.attribution_improvements.push(improvement);
        
        console.log(`   ✅ ATTRIBUTION FOUND: ${attributionResult.method} (${attributionResult.score} points)`);
        console.log(`   🎯 Landing Page: ${attributionResult.landing_page || 'Not specified'}`);
        console.log(`   📈 Improvement: ${improvement.improvement_type} (+${improvement.score_improvement} points)`);
      } else {
        console.log(`   ❌ No attribution found through any method`);
      }

      // Progress update every 10 conversions
      if ((i + 1) % 10 === 0) {
        console.log(`\n📊 PROGRESS: ${i + 1}/${conversionsInRange.length} conversions analyzed`);
        console.log(`   📈 Success Rate: ${Math.round((results.recovery_successful / results.recovery_attempted) * 100)}%`);
      }
    }

    // Final statistics
    results.analysis_summary = {
      attribution_rate_before: Math.round((results.currently_attributed / results.total_conversions_analyzed) * 100),
      potential_attribution_rate: Math.round(((results.currently_attributed + results.recovery_successful) / results.total_conversions_analyzed) * 100),
      improvements_possible: results.recovery_successful,
      improvement_percentage: Math.round((results.recovery_successful / results.total_conversions_analyzed) * 100)
    };

    return results;
  }

  try {
    const analysisResults = await processConversionsJuneAnalysis();
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'June 23-30 conversion analysis completed',
        results: analysisResults,
        script_info: {
          version: 'June Analysis - Fixed Field Mapping',
          compatible_with: 'Current track.js data structure',
          fixes_applied: [
            'Corrected field name mapping (session_id, primary_ip, conversion_ip, pageview_ip)',
            'Removed dependency on device signatures (not stored in conversion records)',
            'Fixed IP extraction logic',
            'Added comprehensive date filtering for June 23-30'
          ]
        }
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
