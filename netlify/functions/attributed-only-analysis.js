// File: netlify/functions/attributed-only-analysis.js
// Analyze ONLY the conversions that were successfully attributed
// Shows what the attribution system was capturing before track.js fixes

const handler = async (event, context) => {
  console.log('🎯 Analyzing ONLY attributed conversions June 23-30...');
  
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

    // Date range
    const startDate = new Date('2025-06-23T00:00:00.000Z');
    const endDate = new Date('2025-06-30T23:59:59.999Z');
    const startTimestamp = startDate.getTime();
    const endTimestamp = endDate.getTime();

    console.log(`📅 Analyzing ATTRIBUTED conversions from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    // Get conversion keys using SCAN with cursor iteration
    let allKeys = [];
    
    try {
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
      
      console.log(`🔍 Found ${allKeys.length} total conversion keys`);
    } catch (error) {
      console.error('❌ SCAN failed:', error.message);
    }

    // Process ONLY attributed conversions
    const attributedConversions = [];
    const methodBreakdown = {};
    const sourceBreakdown = {};
    const campaignBreakdown = {};
    const landingPageBreakdown = {};
    const dailyBreakdown = {};
    const trafficSourceDetails = {};
    
    let totalConversions = 0;
    let attributedCount = 0;
    let totalRevenue = 0;
    
    for (const key of allKeys) {
      try {
        const conversionResult = await redis(`get/${key}`);
        const conversionData = conversionResult.result;
        if (!conversionData) continue;
        
        let conversion;
        try {
          conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
        } catch (parseError) {
          continue;
        }
        
        if (!conversion.timestamp) continue;
        
        const conversionTimestamp = new Date(conversion.timestamp).getTime();
        
        // Check if conversion is in our date range
        if (conversionTimestamp >= startTimestamp && conversionTimestamp <= endTimestamp) {
          totalConversions++;
          
          // ONLY PROCESS ATTRIBUTED CONVERSIONS
          if (conversion.attribution_found === true) {
            attributedCount++;
            
            const orderTotal = parseFloat(conversion.order_total) || 0;
            totalRevenue += orderTotal;
            
            const conversionDate = new Date(conversion.timestamp).toISOString().split('T')[0];
            
            // Detailed conversion info
            const conversionInfo = {
              email: conversion.email,
              timestamp: conversion.timestamp,
              date: conversionDate,
              order_total: orderTotal,
              attribution_method: conversion.attribution_method || 'unknown',
              attribution_score: conversion.attribution_score || 0,
              source: conversion.source || 'direct',
              landing_page: conversion.landing_page || 'none',
              utm_source: conversion.utm_source || null,
              utm_medium: conversion.utm_medium || null,
              utm_campaign: conversion.utm_campaign || null,
              utm_content: conversion.utm_content || null,
              utm_term: conversion.utm_term || null,
              campaign: conversion.campaign || null,
              medium: conversion.medium || null,
              // Extract domain from landing page
              landing_domain: conversion.landing_page ? 
                (() => {
                  try {
                    return new URL(conversion.landing_page).hostname;
                  } catch {
                    return 'invalid-url';
                  }
                })() : 'none'
            };
            
            attributedConversions.push(conversionInfo);
            
            // Track attribution methods
            const method = conversion.attribution_method || 'unknown';
            methodBreakdown[method] = (methodBreakdown[method] || 0) + 1;
            
            // Track sources with details
            const source = conversion.source || 'direct';
            sourceBreakdown[source] = (sourceBreakdown[source] || 0) + 1;
            
            // Track detailed traffic source info
            if (!trafficSourceDetails[source]) {
              trafficSourceDetails[source] = {
                count: 0,
                attribution_methods: {},
                landing_domains: {},
                campaigns: {},
                sample_conversions: []
              };
            }
            trafficSourceDetails[source].count++;
            trafficSourceDetails[source].attribution_methods[method] = 
              (trafficSourceDetails[source].attribution_methods[method] || 0) + 1;
            trafficSourceDetails[source].landing_domains[conversionInfo.landing_domain] = 
              (trafficSourceDetails[source].landing_domains[conversionInfo.landing_domain] || 0) + 1;
            
            if (conversion.utm_campaign || conversion.campaign) {
              const campaign = conversion.utm_campaign || conversion.campaign;
              trafficSourceDetails[source].campaigns[campaign] = 
                (trafficSourceDetails[source].campaigns[campaign] || 0) + 1;
            }
            
            if (trafficSourceDetails[source].sample_conversions.length < 3) {
              trafficSourceDetails[source].sample_conversions.push({
                email: conversion.email,
                timestamp: conversion.timestamp,
                method: method,
                score: conversion.attribution_score,
                landing_page: conversion.landing_page
              });
            }
            
            // Track campaigns
            const campaign = conversion.utm_campaign || conversion.campaign || 'none';
            campaignBreakdown[campaign] = (campaignBreakdown[campaign] || 0) + 1;
            
            // Track landing pages
            const landingPage = conversionInfo.landing_domain;
            landingPageBreakdown[landingPage] = (landingPageBreakdown[landingPage] || 0) + 1;
            
            // Track daily breakdown
            if (!dailyBreakdown[conversionDate]) {
              dailyBreakdown[conversionDate] = {
                conversions: [],
                methods: {},
                sources: {},
                revenue: 0
              };
            }
            dailyBreakdown[conversionDate].conversions.push(conversionInfo);
            dailyBreakdown[conversionDate].methods[method] = 
              (dailyBreakdown[conversionDate].methods[method] || 0) + 1;
            dailyBreakdown[conversionDate].sources[source] = 
              (dailyBreakdown[conversionDate].sources[source] || 0) + 1;
            dailyBreakdown[conversionDate].revenue += orderTotal;
            
            console.log(`✅ ATTRIBUTED: ${conversion.email} - ${method} (${conversion.attribution_score}pts) - ${source}`);
          }
        }
      } catch (error) {
        console.log(`⚠️ Error processing conversion ${key}:`, error.message);
      }
    }
    
    // Sort conversions by timestamp
    attributedConversions.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    // Calculate top performers
    const topMethods = Object.entries(methodBreakdown)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 10);
      
    const topSources = Object.entries(sourceBreakdown)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 10);
      
    const topCampaigns = Object.entries(campaignBreakdown)
      .sort(([,a], [,b]) => b - a)
      .filter(([campaign]) => campaign !== 'none')
      .slice(0, 10);
    
    // Calculate daily stats
    const dailyStats = {};
    for (const date in dailyBreakdown) {
      const dayData = dailyBreakdown[date];
      const dayConversions = dayData.conversions;
      const dayRevenue = dayData.revenue;
      
      const topMethod = Object.entries(dayData.methods)
        .sort(([,a], [,b]) => b - a)[0];
      const topSource = Object.entries(dayData.sources)
        .sort(([,a], [,b]) => b - a)[0];
      
      dailyStats[date] = {
        conversions: dayConversions.length,
        revenue: dayRevenue,
        avg_order_value: dayConversions.length > 0 ? dayRevenue / dayConversions.length : 0,
        top_method: topMethod ? topMethod[0] : 'none',
        top_method_count: topMethod ? topMethod[1] : 0,
        top_source: topSource ? topSource[0] : 'none',
        top_source_count: topSource ? topSource[1] : 0,
        attribution_methods: dayData.methods,
        traffic_sources: dayData.sources,
        sample_conversions: dayConversions.slice(0, 3)
      };
    }
    
    // Summary statistics
    const summary = {
      analysis_period: {
        start: startDate.toISOString().split('T')[0],
        end: endDate.toISOString().split('T')[0]
      },
      total_conversions_in_period: totalConversions,
      attributed_conversions: attributedCount,
      attribution_success_rate: totalConversions > 0 ? Math.round((attributedCount / totalConversions) * 100) : 0,
      total_revenue: totalRevenue,
      avg_order_value: attributedCount > 0 ? totalRevenue / attributedCount : 0,
      attribution_methods_used: Object.keys(methodBreakdown).length,
      traffic_sources_captured: Object.keys(sourceBreakdown).length,
      campaigns_tracked: Object.keys(campaignBreakdown).filter(c => c !== 'none').length,
      landing_domains_tracked: Object.keys(landingPageBreakdown).filter(d => d !== 'none').length
    };

    // Performance insights
    const insights = {
      highest_scoring_method: topMethods[0] || ['none', 0],
      most_successful_source: topSources[0] || ['direct', 0],
      most_active_day: Object.entries(dailyStats).reduce((best, [date, stats]) => 
        stats.conversions > (best[1]?.conversions || 0) ? [date, stats] : best, ['none', { conversions: 0 }]
      ),
      attribution_quality: {
        session_based: (methodBreakdown['session_id_match'] || 0),
        ip_based: (methodBreakdown['primary_ip_match'] || 0) + (methodBreakdown['conversion_ip_match'] || 0) + (methodBreakdown['pageview_ip_match'] || 0),
        device_based: (methodBreakdown['device_signature_match'] || 0) + (methodBreakdown['device_signature_match_corrected'] || 0),
        screen_based: (methodBreakdown['screen_hash_match'] || 0),
        geographic_based: (methodBreakdown['geographic_match'] || 0)
      }
    };

    console.log('📊 Attributed conversions analysis complete');
    console.log(`✅ Found ${attributedCount} attributed conversions out of ${totalConversions} total`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'Attributed conversions analysis completed',
        results: {
          summary: summary,
          attributed_conversions: attributedConversions,
          breakdown: {
            by_method: methodBreakdown,
            by_source: sourceBreakdown,
            by_campaign: campaignBreakdown,
            by_landing_page: landingPageBreakdown,
            by_date: dailyStats
          },
          traffic_source_details: trafficSourceDetails,
          top_performers: {
            methods: topMethods,
            sources: topSources,
            campaigns: topCampaigns
          },
          insights: insights
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
