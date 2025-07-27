// Attribution Analytics Dashboard - Phase 3
// Path: netlify/functions/attribution-analytics.js
// Purpose: Searchable attribution analytics with summary tables and drill-down capability

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    // Parse request body
    const requestData = JSON.parse(event.body || '{}');
    const { 
      analysis_type, 
      start_date, 
      end_date, 
      attribution_model = 'last_touch', // 'first_touch' or 'last_touch'
      sort_by = 'conversions', // 'conversions', 'source', 'landing_page'
      sort_order = 'desc', // 'asc' or 'desc'
      email,
      timestamp
    } = requestData;
    
    if (!analysis_type) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Missing required field: analysis_type (summary_table or drill_down)' 
        })
      };
    }
    
    console.log(`📊 Starting attribution analytics: ${analysis_type}`);
    
    if (analysis_type === 'summary_table') {
      if (!start_date || !end_date) {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ 
            error: 'Missing required fields for summary_table: start_date and end_date' 
          })
        };
      }
      
      console.log(`📅 Date range: ${start_date} to ${end_date}, Attribution model: ${attribution_model}`);
      
      // Get attribution summary table
      const summaryResult = await generateAttributionSummaryTable(
        redis, 
        start_date, 
        end_date, 
        attribution_model, 
        sort_by, 
        sort_order
      );
      
      const processingTime = Date.now() - startTime;
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          analysis_type: 'summary_table',
          query_parameters: {
            start_date,
            end_date,
            attribution_model,
            sort_by,
            sort_order
          },
          summary_table: summaryResult.table_data,
          summary_stats: {
            total_conversions: summaryResult.total_conversions,
            total_revenue: summaryResult.total_revenue,
            unique_sources: summaryResult.unique_sources,
            unique_landing_pages: summaryResult.unique_landing_pages,
            date_range_days: summaryResult.date_range_days
          },
          processing_time_ms: processingTime
        })
      };
      
    } else if (analysis_type === 'drill_down') {
      if (!email || !timestamp) {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ 
            error: 'Missing required fields for drill_down: email and timestamp' 
          })
        };
      }
      
      console.log(`🔍 Drill-down for: ${email} at ${timestamp}`);
      
      // Get individual customer journey
      const drillDownResult = await getCustomerJourneyDrillDown(redis, email, timestamp);
      
      if (!drillDownResult) {
        return {
          statusCode: 404,
          headers,
          body: JSON.stringify({ 
            error: 'Attribution data not found for this conversion',
            email,
            timestamp
          })
        };
      }
      
      const processingTime = Date.now() - startTime;
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          analysis_type: 'drill_down',
          query_parameters: { email, timestamp },
          customer_journey: drillDownResult,
          processing_time_ms: processingTime
        })
      };
    }
    
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ 
        error: 'Invalid analysis_type. Use "summary_table" or "drill_down"' 
      })
    };
    
  } catch (error) {
    console.error('❌ Attribution analytics failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Attribution analytics failed', 
        message: error.message 
      })
    };
  }
};

// Generate attribution summary table for date range
async function generateAttributionSummaryTable(redis, startDate, endDate, attributionModel, sortBy, sortOrder) {
  console.log(`📊 Generating ${attributionModel} attribution summary table...`);
  
  const startTime = Date.now();
  const attributionData = new Map(); // Key: "source|landing_page", Value: {conversions, revenue, samples}
  let totalConversions = 0;
  let totalRevenue = 0;
  const uniqueSources = new Set();
  const uniqueLandingPages = new Set();
  
  // Calculate date range
  const start = new Date(startDate);
  const end = new Date(endDate);
  const dateRangeDays = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
  
  console.log(`🔍 Scanning attribution data for ${dateRangeDays} day range...`);
  
  // Scan for attribution results in date range
  let cursor = '0';
  let scannedKeys = 0;
  let processedAttributions = 0;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/multi_touch_attribution:*/count/200`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      scannedKeys += keys.length;
      
      console.log(`📦 Processing ${keys.length} attribution keys (cursor: ${cursor})`);
      
      // Process attribution keys in batches
      const batchSize = 20;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const attributionResult = await redis(`get/${key}`, 2000);
            
            if (attributionResult?.result) {
              const attribution = JSON.parse(decodeURIComponent(attributionResult.result));
              
              // Check if conversion is in date range
              const conversionDate = new Date(attribution.conversion.timestamp);
              if (conversionDate >= start && conversionDate <= end) {
                return attribution;
              }
            }
          } catch (keyError) {
            console.log(`⚠️ Error processing key ${key}: ${keyError.message}`);
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validAttributions = batchResults.filter(result => result !== null);
        
        // Process valid attributions
        validAttributions.forEach(attribution => {
          processedAttributions++;
          
          // Get touch point based on attribution model
          let touchPoint;
          if (attributionModel === 'first_touch' && attribution.attribution_summary.first_touch) {
            touchPoint = attribution.attribution_summary.first_touch;
          } else if (attributionModel === 'last_touch' && attribution.attribution_summary.last_touch) {
            touchPoint = attribution.attribution_summary.last_touch;
          } else {
            // Fallback to conversion data if no touch points available
            touchPoint = {
              source: 'direct',
              landing_page: attribution.conversion.landing_page || 'unknown'
            };
          }
          
          const source = touchPoint.source || 'direct';
          const landingPage = touchPoint.landing_page || 'unknown';
          const revenue = attribution.conversion.order_total || 0;
          
          uniqueSources.add(source);
          uniqueLandingPages.add(landingPage);
          
          // Group by source and landing page
          const groupKey = `${source}|${landingPage}`;
          
          if (!attributionData.has(groupKey)) {
            attributionData.set(groupKey, {
              source: source,
              landing_page: landingPage,
              conversions: 0,
              revenue: 0,
              conversion_samples: []
            });
          }
          
          const groupData = attributionData.get(groupKey);
          groupData.conversions++;
          groupData.revenue += revenue;
          
          // Keep samples for drill-down (up to 5 per group)
          if (groupData.conversion_samples.length < 5) {
            groupData.conversion_samples.push({
              email: attribution.conversion.email,
              timestamp: attribution.conversion.timestamp,
              order_total: revenue,
              touchpoints: attribution.attribution_summary.total_touchpoints || 0
            });
          }
          
          totalConversions++;
          totalRevenue += revenue;
        });
      }
      
    } catch (scanError) {
      console.log(`⚠️ Scan error: ${scanError.message}`);
      break;
    }
    
    // Safety check - don't scan forever
    if (scannedKeys > 10000) {
      console.log(`🛑 Safety limit: scanned ${scannedKeys} keys, stopping`);
      break;
    }
    
  } while (cursor !== '0');
  
  console.log(`📊 Scan complete: ${scannedKeys} keys scanned, ${processedAttributions} attributions processed`);
  
  // Convert to table format
  const tableData = Array.from(attributionData.values()).map(group => ({
    source: group.source,
    landing_page: group.landing_page,
    number_of_conversions: group.conversions,
    conversion_rate: totalConversions > 0 ? `${((group.conversions / totalConversions) * 100).toFixed(1)}%` : '0%',
    revenue: group.revenue,
    avg_order_value: group.conversions > 0 ? Math.round(group.revenue / group.conversions) : 0,
    revenue_share: totalRevenue > 0 ? `${((group.revenue / totalRevenue) * 100).toFixed(1)}%` : '0%',
    conversion_samples: group.conversion_samples
  }));
  
  // Sort table data
  tableData.sort((a, b) => {
    let aValue, bValue;
    
    switch (sortBy) {
      case 'conversions':
        aValue = a.number_of_conversions;
        bValue = b.number_of_conversions;
        break;
      case 'source':
        aValue = a.source.toLowerCase();
        bValue = b.source.toLowerCase();
        break;
      case 'landing_page':
        aValue = a.landing_page.toLowerCase();
        bValue = b.landing_page.toLowerCase();
        break;
      case 'revenue':
        aValue = a.revenue;
        bValue = b.revenue;
        break;
      default:
        aValue = a.number_of_conversions;
        bValue = b.number_of_conversions;
    }
    
    if (sortOrder === 'desc') {
      return bValue > aValue ? 1 : bValue < aValue ? -1 : 0;
    } else {
      return aValue > bValue ? 1 : aValue < bValue ? -1 : 0;
    }
  });
  
  const processingTime = Date.now() - startTime;
  console.log(`✅ Summary table generated in ${processingTime}ms: ${tableData.length} rows`);
  
  return {
    table_data: tableData,
    total_conversions: totalConversions,
    total_revenue: totalRevenue,
    unique_sources: uniqueSources.size,
    unique_landing_pages: uniqueLandingPages.size,
    date_range_days: dateRangeDays,
    processing_time_ms: processingTime
  };
}

// Get individual customer journey for drill-down
async function getCustomerJourneyDrillDown(redis, email, timestamp) {
  console.log(`🔍 Getting customer journey drill-down for: ${email}`);
  
  try {
    const attributionKey = `multi_touch_attribution:${email}:${timestamp}`;
    const attributionResult = await redis(`get/${attributionKey}`, 3000);
    
    if (!attributionResult?.result) {
      console.log('❌ No attribution data found');
      return null;
    }
    
    const attribution = JSON.parse(decodeURIComponent(attributionResult.result));
    
    // Format customer journey for table display
    const formattedJourney = attribution.customer_journey.map((pageview, index) => ({
      touchpoint_number: index + 1,
      timestamp: pageview.timestamp,
      date: new Date(pageview.timestamp).toLocaleDateString(),
      time: new Date(pageview.timestamp).toLocaleTimeString(),
      source: pageview.source || 'direct',
      landing_page: pageview.landing_page || 'unknown',
      utm_campaign: pageview.utm_campaign || null,
      utm_source: pageview.utm_source || null,
      utm_medium: pageview.utm_medium || null,
      ip_address: pageview.ip_address || 'unknown',
      session_id: pageview.session_id || null,
      attribution_method: pageview.attribution_method || 'unknown',
      screen_resolution: pageview.screen_resolution || null,
      referrer_url: pageview.referrer_url || null
    }));
    
    console.log(`✅ Customer journey found: ${formattedJourney.length} touchpoints`);
    
    return {
      conversion_info: {
        email: attribution.conversion.email,
        timestamp: attribution.conversion.timestamp,
        conversion_date: new Date(attribution.conversion.timestamp).toLocaleDateString(),
        conversion_time: new Date(attribution.conversion.timestamp).toLocaleTimeString(),
        order_total: attribution.conversion.order_total,
        conversion_ip: attribution.conversion.conversion_ip,
        primary_ip: attribution.conversion.primary_ip,
        session_id: attribution.conversion.ssid,
        landing_page: attribution.conversion.landing_page
      },
      
      attribution_summary: {
        total_touchpoints: attribution.attribution_summary.total_touchpoints,
        journey_duration_days: attribution.attribution_summary.journey_duration_days,
        attribution_methods_used: attribution.attribution_summary.attribution_methods_used,
        unique_sources: attribution.attribution_summary.unique_sources || [],
        unique_campaigns: attribution.attribution_summary.unique_campaigns || [],
        attribution_confidence_score: attribution.attribution_summary.attribution_confidence?.score || 0,
        first_touch: attribution.attribution_summary.first_touch,
        last_touch: attribution.attribution_summary.last_touch
      },
      
      customer_journey_table: formattedJourney,
      
      technical_details: {
        storage_key: attributionKey,
        stored_at: attribution.storage_metadata?.stored_at,
        attribution_version: attribution.storage_metadata?.attribution_version
      }
    };
    
  } catch (error) {
    console.log(`❌ Error getting customer journey: ${error.message}`);
    return null;
  }
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
