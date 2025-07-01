// File: netlify/functions/attribution-recovery-3phase.js
// PROPERLY CONFIGURED VERSION with all fixes applied
// Tests specific conversions through full 8-tier attribution logic

const handler = async (event, context) => {
  console.log('🔄 Starting Attribution Recovery 3-Phase (Configured Version)...');
  
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

    // Get specific conversions or all unattributed from June 30th
    const targetEmails = [
      'advlacademia@gmail.com',
      'edwin@foxhoundadvertising.com', 
      'zikouba2022@gmail.com'
    ];

    console.log(`🎯 Looking for specific conversions: ${targetEmails.join(', ')}`);

    // Phase 1: Find target conversions in Redis
    let allKeys = [];
    let cursor = '0';
    
    do {
      const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
      if (result.result && result.result[1]) {
        cursor = result.result[0];
        const keys = result.result[1];
        allKeys = allKeys.concat(keys);
        if (allKeys.length > 10000) break;
      } else {
        break;
      }
    } while (cursor !== '0');

    console.log(`🔍 Scanning ${allKeys.length} conversion keys...`);

    // Find target conversions
    const targetConversions = [];
    const allUnattributed = [];

    for (const key of allKeys) {
      try {
        const conversionResult = await redis(`get/${key}`);
        const conversionData = conversionResult.result;
        if (!conversionData) continue;
        
        let conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
        
        if (!conversion.email || !conversion.timestamp) continue;

        // Check if this is a target conversion or unattributed June 30th
        const conversionDate = new Date(conversion.timestamp);
        const isJune30 = conversionDate.toISOString().startsWith('2025-06-30');
        const isTargetEmail = targetEmails.includes(conversion.email);
        const isUnattributed = !conversion.attribution_found || conversion.attribution_found === false;

        if (isTargetEmail || (isJune30 && isUnattributed)) {
          const conversionInfo = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            redis_key: key,
            attribution_found: conversion.attribution_found,
            landing_page: conversion.landing_page,
            source: conversion.source,
            order_total: conversion.order_total,
            // Attribution parameters that might exist
            session_id: conversion.session_id || conversion.ssid || null,
            primary_ip: conversion.primary_ip || conversion.PIP || null,
            conversion_ip: conversion.conversion_ip || conversion.CIP || null,
            pageview_ip: conversion.pageview_ip || conversion.IP || null,
            device_signature: conversion.device_signature || conversion.dsig || null,
            screen_value: conversion.SVV || conversion.SVVV || conversion.screen_value || null,
            webgl_signature: conversion.gsig || conversion.webgl_signature || null
          };

          if (isTargetEmail) {
            targetConversions.push(conversionInfo);
            console.log(`✅ Found target: ${conversion.email} - ${conversion.timestamp}`);
          } else {
            allUnattributed.push(conversionInfo);
          }
        }
      } catch (error) {
        console.log(`⚠️ Error processing ${key}: ${error.message}`);
      }
    }

    console.log(`📊 Found ${targetConversions.length} target conversions, ${allUnattributed.length} other unattributed June 30th conversions`);

    // Phase 2: Define 8-tier attribution priority system
    const attributionPriorities = [
      { name: 'session_id_match', field: 'session_id', keyPrefix: 'attribution_session_', points: 300, priority: 1 },
      { name: 'primary_ip_match', field: 'primary_ip', keyPrefix: 'attribution_ip_', points: 280, priority: 2 },
      { name: 'conversion_ip_match', field: 'conversion_ip', keyPrefix: 'attribution_ip_', points: 260, priority: 3 },
      { name: 'pageview_ip_match', field: 'pageview_ip', keyPrefix: 'attribution_ip_', points: 240, priority: 4 },
      { name: 'device_signature_match', field: 'device_signature', keyPrefix: 'attribution_fp_', points: 220, priority: 5 },
      { name: 'screen_hash_match', field: 'screen_value', keyPrefix: 'attribution_screen_', points: 200, priority: 6 },
      { name: 'webgl_match', field: 'webgl_signature', keyPrefix: 'attribution_webgl_', points: 180, priority: 7 },
      { name: 'geographic_match', field: 'geographic_hash', keyPrefix: 'attribution_geo_', points: 100, priority: 8 }
    ];

    // Phase 3: Attempt attribution recovery
    const recoveryResults = [];
    const conversionsToAnalyze = targetConversions.length > 0 ? targetConversions : allUnattributed.slice(0, 10);

    console.log(`🔬 Analyzing ${conversionsToAnalyze.length} conversions for attribution recovery...`);

    for (const conversion of conversionsToAnalyze) {
      console.log(`\n🎯 ANALYZING: ${conversion.email} (${conversion.timestamp})`);
      
      const conversionResult = {
        email: conversion.email,
        timestamp: conversion.timestamp,
        current_attribution: conversion.attribution_found,
        current_landing_page: conversion.landing_page,
        recovery_attempts: [],
        best_match: null,
        recovery_successful: false
      };

      // Extract available parameters
      const params = {
        session_id: conversion.session_id,
        primary_ip: conversion.primary_ip,
        conversion_ip: conversion.conversion_ip,
        pageview_ip: conversion.pageview_ip,
        device_signature: conversion.device_signature,
        screen_value: conversion.screen_value,
        webgl_signature: conversion.webgl_signature
      };

      console.log(`📋 Available params:`, Object.entries(params).filter(([k,v]) => v).map(([k,v]) => `${k}=${v ? 'YES' : 'NO'}`).join(', '));

      // Try each attribution method in priority order
      for (const priority of attributionPriorities) {
        const fieldValue = params[priority.field];
        
        if (!fieldValue) {
          console.log(`   ❌ ${priority.name}: No ${priority.field} available`);
          continue;
        }

        // Generate lookup key with correct format
        let lookupKey;
        if (priority.keyPrefix === 'attribution_ip_') {
          // Correct IP key format: IPv4 keeps dots, IPv6 converts colons to underscores
          if (fieldValue.includes(':')) {
            // IPv6
            lookupKey = `${priority.keyPrefix}${fieldValue.replace(/:/g, '_')}`;
          } else {
            // IPv4
            lookupKey = `${priority.keyPrefix}${fieldValue}`;
          }
        } else {
          lookupKey = `${priority.keyPrefix}${fieldValue}`;
        }

        console.log(`   🔑 ${priority.name}: Looking up ${lookupKey}`);

        try {
          const attributionResult = await redis(`get/${lookupKey}`);
          const attributionData = attributionResult.result;

          const attempt = {
            method: priority.name,
            field: priority.field,
            value: fieldValue,
            lookup_key: lookupKey,
            points: priority.points,
            priority: priority.priority,
            found: !!attributionData,
            data: attributionData ? (typeof attributionData === 'string' ? attributionData : JSON.stringify(attributionData)) : null
          };

          conversionResult.recovery_attempts.push(attempt);

          if (attributionData) {
            console.log(`   ✅ ${priority.name}: FOUND attribution data!`);
            console.log(`      📄 Data preview: ${attributionData.substring(0, 100)}...`);
            
            // This is the best match (highest priority found)
            if (!conversionResult.best_match) {
              conversionResult.best_match = attempt;
              conversionResult.recovery_successful = true;
            }
          } else {
            console.log(`   ❌ ${priority.name}: No attribution data found`);
          }
        } catch (error) {
          console.log(`   ⚠️ ${priority.name}: Error during lookup - ${error.message}`);
          conversionResult.recovery_attempts.push({
            method: priority.name,
            field: priority.field,
            value: fieldValue,
            lookup_key: lookupKey,
            points: priority.points,
            priority: priority.priority,
            found: false,
            error: error.message
          });
        }

        // If we found attribution with this method, we can stop (highest priority wins)
        if (conversionResult.best_match) {
          console.log(`   🎉 Recovery successful with ${priority.name} (${priority.points} points)`);
          break;
        }
      }

      // Summary for this conversion
      if (conversionResult.recovery_successful) {
        console.log(`✅ ${conversion.email}: RECOVERED via ${conversionResult.best_match.method}`);
      } else {
        console.log(`❌ ${conversion.email}: No attribution found with any method`);
      }

      recoveryResults.push(conversionResult);
    }

    // Final summary
    const successfulRecoveries = recoveryResults.filter(r => r.recovery_successful);
    const failedRecoveries = recoveryResults.filter(r => !r.recovery_successful);

    console.log(`\n📊 RECOVERY SUMMARY:`);
    console.log(`✅ Successful recoveries: ${successfulRecoveries.length}`);
    console.log(`❌ Failed recoveries: ${failedRecoveries.length}`);

    // Method breakdown
    const methodBreakdown = {};
    successfulRecoveries.forEach(r => {
      const method = r.best_match.method;
      methodBreakdown[method] = (methodBreakdown[method] || 0) + 1;
    });

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'Attribution recovery analysis completed',
        results: {
          target_conversions_found: targetConversions.length,
          total_analyzed: conversionsToAnalyze.length,
          successful_recoveries: successfulRecoveries.length,
          failed_recoveries: failedRecoveries.length,
          recovery_rate: conversionsToAnalyze.length > 0 ? 
            Math.round((successfulRecoveries.length / conversionsToAnalyze.length) * 100) : 0,
          method_breakdown: methodBreakdown,
          detailed_results: recoveryResults,
          configuration: {
            redis_key_pattern: 'conversions:*',
            ip_key_format: 'IPv4_with_dots_IPv6_with_underscores',
            attribution_priorities: attributionPriorities.length,
            field_mapping: 'session_id|primary_ip|conversion_ip|pageview_ip|device_signature|screen_value|webgl_signature'
          }
        }
      })
    };

  } catch (error) {
    console.error('❌ Attribution recovery error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Attribution recovery failed', 
        details: error.message 
      })
    };
  }
};

module.exports = { handler };
