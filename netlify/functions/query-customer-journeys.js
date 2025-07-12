// netlify/functions/query-customer-journeys.js
// Customer Journey Analytics & Query System
// Analyzes and retrieves customer journey data for business intelligence

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
    const redis = initializeRedis();
    
    if (event.httpMethod === 'GET') {
      // Return journey system status and analytics overview
      const systemStatus = await getJourneySystemStatus(redis);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify(systemStatus)
      };
    }
    
    if (event.httpMethod === 'POST') {
      const body = JSON.parse(event.body || '{}');
      const {
        query_type = 'analytics',
        email,
        order_id,
        date_range_days = 7,
        include_touchpoints = false,
        attribution_analysis = false
      } = body;
      
      console.log(`🔍 Journey Query: ${query_type} for ${date_range_days} days`);
      
      let result;
      
      switch (query_type) {
        case 'analytics':
          result = await getJourneyAnalytics(redis, date_range_days);
          break;
          
        case 'customer_journey':
          if (!email && !order_id) {
            return {
              statusCode: 400,
              headers,
              body: JSON.stringify({ error: 'email or order_id required for customer journey query' })
            };
          }
          result = await getCustomerJourney(redis, email, order_id, include_touchpoints);
          break;
          
        case 'attribution_comparison':
          result = await getAttributionComparison(redis, date_range_days);
          break;
          
        case 'journey_patterns':
          result = await getJourneyPatterns(redis, date_range_days);
          break;
          
        default:
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Invalid query_type. Use: analytics, customer_journey, attribution_comparison, journey_patterns' })
          };
      }
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify(result)
      };
    }
    
  } catch (error) {
    console.error('❌ Journey query error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: error.message })
    };
  }
};

// Get journey system status and overview
async function getJourneySystemStatus(redis) {
  try {
    // Get journey analytics index
    const analyticsResult = await redis(`get/customer_journey_analytics`);
    
    if (analyticsResult?.result) {
      const analytics = JSON.parse(decodeURIComponent(analyticsResult.result));
      
      const now = new Date();
      const createdTime = new Date(analytics.created_at);
      const ageMinutes = Math.round((now - createdTime) / (1000 * 60));
      
      return {
        system_status: 'operational',
        journey_data_available: true,
        last_reconstruction: analytics.created_at,
        data_age_minutes: ageMinutes,
        data_freshness: ageMinutes < 60 ? 'fresh' : ageMinutes < 180 ? 'acceptable' : 'stale',
        
        journey_overview: {
          total_journeys: analytics.total_journeys,
          journeys_with_multiple_touchpoints: analytics.journeys_with_multiple_touchpoints,
          cross_session_journeys: analytics.cross_session_journeys,
          cross_device_journeys: analytics.cross_device_journeys,
          avg_touchpoints_per_journey: analytics.avg_touchpoints?.toFixed(2),
          avg_journey_span_hours: analytics.avg_journey_span_hours?.toFixed(2),
          total_conversion_value: analytics.total_conversion_value
        },
        
        capabilities: {
          customer_journey_lookup: true,
          attribution_comparison: true,
          journey_pattern_analysis: true,
          multi_touch_attribution_ready: analytics.journeys_with_multiple_touchpoints > 0,
          cross_device_tracking: analytics.cross_device_journeys > 0
        },
        
        recommendations: ageMinutes > 180 ? ['Run journey reconstruction to refresh data'] : []
      };
    } else {
      return {
        system_status: 'needs_initialization',
        journey_data_available: false,
        message: 'No journey data found',
        recommendations: ['Run customer journey reconstruction to build journey database']
      };
    }
    
  } catch (error) {
    return {
      system_status: 'error',
      error: error.message,
      recommendations: ['Check Redis connectivity and run journey reconstruction']
    };
  }
}

// Get comprehensive journey analytics
async function getJourneyAnalytics(redis, dateRangeDays) {
  const analyticsStartTime = Date.now();
  console.log(`📊 Building journey analytics for ${dateRangeDays} days...`);
  
  // Get all journey keys
  const journeyKeys = await findJourneyKeys(redis, dateRangeDays);
  console.log(`🔍 Found ${journeyKeys.length} journey records`);
  
  if (journeyKeys.length === 0) {
    return {
      success: false,
      message: 'No journey data found',
      recommendations: ['Run customer journey reconstruction first']
    };
  }
  
  // Load and analyze journeys
  const journeyAnalysis = await analyzeJourneys(redis, journeyKeys);
  
  const processingTime = Date.now() - analyticsStartTime;
  
  return {
    success: true,
    analytics_timestamp: new Date().toISOString(),
    date_range_days: dateRangeDays,
    processing_time_ms: processingTime,
    
    journey_overview: {
      total_journeys_analyzed: journeyAnalysis.total_journeys,
      total_conversion_value: journeyAnalysis.total_conversion_value,
      avg_journey_value: journeyAnalysis.avg_journey_value,
      conversion_rate: '100%' // All journeys have conversions by definition
    },
    
    touchpoint_analysis: {
      avg_touchpoints_per_journey: journeyAnalysis.avg_touchpoints,
      total_touchpoints: journeyAnalysis.total_touchpoints,
      single_touchpoint_journeys: journeyAnalysis.single_touchpoint_journeys,
      multi_touchpoint_journeys: journeyAnalysis.multi_touchpoint_journeys,
      max_touchpoints_in_journey: journeyAnalysis.max_touchpoints
    },
    
    journey_characteristics: {
      cross_session_journeys: journeyAnalysis.cross_session_journeys,
      cross_device_journeys: journeyAnalysis.cross_device_journeys,
      avg_journey_span_hours: journeyAnalysis.avg_journey_span_hours,
      max_journey_span_hours: journeyAnalysis.max_journey_span_hours
    },
    
    attribution_insights: {
      first_click_sources: journeyAnalysis.first_click_sources,
      last_click_sources: journeyAnalysis.last_click_sources,
      attribution_method_distribution: journeyAnalysis.attribution_methods,
      high_confidence_attributions: journeyAnalysis.high_confidence_attributions
    },
    
    business_intelligence: {
      revenue_by_first_click_source: journeyAnalysis.revenue_by_first_click,
      revenue_by_last_click_source: journeyAnalysis.revenue_by_last_click,
      top_performing_sources: journeyAnalysis.top_sources,
      journey_value_distribution: journeyAnalysis.value_distribution
    }
  };
}

// Get specific customer journey
async function getCustomerJourney(redis, email, orderId, includeTouchpoints) {
  console.log(`🔍 Looking up customer journey for email: ${email}, order: ${orderId}`);
  
  try {
    // Find journey by email or order ID
    let journey = null;
    
    if (orderId) {
      // Search by order ID (more specific)
      const journeyKeys = await findJourneyKeys(redis, 30); // 30-day search
      
      for (const key of journeyKeys) {
        try {
          const journeyData = await redis(`get/${key}`);
          if (journeyData?.result) {
            const journeyRecord = JSON.parse(decodeURIComponent(journeyData.result));
            if (journeyRecord.conversion_order_id === orderId) {
              journey = journeyRecord;
              break;
            }
          }
        } catch (parseError) {
          continue;
        }
      }
    } else if (email) {
      // Search by email (less specific, might return multiple)
      const journeyKeys = await findJourneyKeys(redis, 30);
      const emailJourneys = [];
      
      for (const key of journeyKeys) {
        try {
          const journeyData = await redis(`get/${key}`);
          if (journeyData?.result) {
            const journeyRecord = JSON.parse(decodeURIComponent(journeyData.result));
            if (journeyRecord.customer_email === email) {
              emailJourneys.push(journeyRecord);
            }
          }
        } catch (parseError) {
          continue;
        }
      }
      
      // Return most recent journey for this email
      if (emailJourneys.length > 0) {
        emailJourneys.sort((a, b) => new Date(b.conversion_timestamp) - new Date(a.conversion_timestamp));
        journey = emailJourneys[0];
      }
    }
    
    if (!journey) {
      return {
        found: false,
        message: 'No journey found for the specified criteria'
      };
    }
    
    // Build response
    const response = {
      found: true,
      journey_id: journey.journey_id,
      customer_email: journey.customer_email,
      conversion_order_id: journey.conversion_order_id,
      conversion_value: journey.conversion_value,
      conversion_timestamp: journey.conversion_timestamp,
      
      journey_summary: {
        journey_span_hours: journey.journey_span_hours,
        total_touchpoints: journey.total_touchpoints,
        unique_sessions: journey.unique_sessions,
        unique_sources: journey.unique_sources,
        cross_session_journey: journey.cross_session_journey,
        cross_device_journey: journey.cross_device_journey
      },
      
      attribution_analysis: {
        first_click_source: journey.first_click_source,
        last_click_source: journey.last_click_source,
        attribution_confidence_avg: journey.attribution_confidence_avg,
        reconstruction_method: journey.reconstruction_method
      }
    };
    
    // Include full touchpoint details if requested
    if (includeTouchpoints && journey.touchpoints) {
      response.touchpoints = journey.touchpoints.map(touchpoint => ({
        touchpoint_id: touchpoint.touchpoint_id,
        timestamp: touchpoint.timestamp,
        landing_page: touchpoint.landing_page,
        source: touchpoint.source,
        medium: touchpoint.medium,
        campaign: touchpoint.campaign,
        attribution_method: touchpoint.attribution_method,
        confidence: touchpoint.confidence,
        touchpoint_position: touchpoint.touchpoint_position,
        is_first_touchpoint: touchpoint.is_first_touchpoint,
        is_last_touchpoint: touchpoint.is_last_touchpoint,
        is_conversion: touchpoint.is_conversion
      }));
    }
    
    return response;
    
  } catch (error) {
    return {
      found: false,
      error: error.message
    };
  }
}

// Get attribution comparison (first-click vs last-click analysis)
async function getAttributionComparison(redis, dateRangeDays) {
  console.log(`🔬 Building attribution comparison for ${dateRangeDays} days...`);
  
  const journeyKeys = await findJourneyKeys(redis, dateRangeDays);
  
  if (journeyKeys.length === 0) {
    return {
      success: false,
      message: 'No journey data found for attribution comparison'
    };
  }
  
  const attributionComparison = {
    first_click_attribution: {},
    last_click_attribution: {},
    source_performance: {},
    attribution_model_differences: []
  };
  
  let totalRevenue = 0;
  let journeysAnalyzed = 0;
  
  // Process journeys for attribution comparison
  for (const key of journeyKeys) {
    try {
      const journeyData = await redis(`get/${key}`);
      if (journeyData?.result) {
        const journey = JSON.parse(decodeURIComponent(journeyData.result));
        
        const firstSource = journey.first_click_source || 'unknown';
        const lastSource = journey.last_click_source || 'unknown';
        const revenue = journey.conversion_value || 0;
        
        // First-click attribution
        if (!attributionComparison.first_click_attribution[firstSource]) {
          attributionComparison.first_click_attribution[firstSource] = { conversions: 0, revenue: 0 };
        }
        attributionComparison.first_click_attribution[firstSource].conversions++;
        attributionComparison.first_click_attribution[firstSource].revenue += revenue;
        
        // Last-click attribution
        if (!attributionComparison.last_click_attribution[lastSource]) {
          attributionComparison.last_click_attribution[lastSource] = { conversions: 0, revenue: 0 };
        }
        attributionComparison.last_click_attribution[lastSource].conversions++;
        attributionComparison.last_click_attribution[lastSource].revenue += revenue;
        
        // Track differences
        if (firstSource !== lastSource) {
          attributionComparison.attribution_model_differences.push({
            journey_id: journey.journey_id,
            first_click_source: firstSource,
            last_click_source: lastSource,
            conversion_value: revenue,
            touchpoints: journey.total_touchpoints
          });
        }
        
        totalRevenue += revenue;
        journeysAnalyzed++;
      }
    } catch (parseError) {
      continue;
    }
  }
  
  // Calculate attribution model impact
  const firstClickTotal = Object.values(attributionComparison.first_click_attribution)
    .reduce((sum, data) => sum + data.revenue, 0);
  const lastClickTotal = Object.values(attributionComparison.last_click_attribution)
    .reduce((sum, data) => sum + data.revenue, 0);
  
  return {
    success: true,
    comparison_timestamp: new Date().toISOString(),
    journeys_analyzed: journeysAnalyzed,
    total_revenue: totalRevenue,
    
    attribution_models: {
      first_click_attribution: attributionComparison.first_click_attribution,
      last_click_attribution: attributionComparison.last_click_attribution
    },
    
    model_comparison: {
      attribution_differences_found: attributionComparison.attribution_model_differences.length,
      percentage_with_different_attribution: ((attributionComparison.attribution_model_differences.length / journeysAnalyzed) * 100).toFixed(2),
      first_click_total_revenue: firstClickTotal,
      last_click_total_revenue: lastClickTotal,
      revenue_difference: Math.abs(firstClickTotal - lastClickTotal)
    },
    
    top_attribution_differences: attributionComparison.attribution_model_differences
      .sort((a, b) => b.conversion_value - a.conversion_value)
      .slice(0, 10),
    
    business_insights: {
      sources_overvalued_by_last_click: getSourcesOvervaluedByLastClick(attributionComparison),
      sources_undervalued_by_last_click: getSourcesUndervaluedByLastClick(attributionComparison),
      multi_touch_journey_percentage: ((attributionComparison.attribution_model_differences.length / journeysAnalyzed) * 100).toFixed(2)
    }
  };
}

// Get journey patterns analysis
async function getJourneyPatterns(redis, dateRangeDays) {
  console.log(`🔍 Analyzing journey patterns for ${dateRangeDays} days...`);
  
  const journeyKeys = await findJourneyKeys(redis, dateRangeDays);
  
  if (journeyKeys.length === 0) {
    return {
      success: false,
      message: 'No journey data found for pattern analysis'
    };
  }
  
  const patterns = {
    common_source_sequences: {},
    journey_length_patterns: {},
    conversion_timing_patterns: {},
    cross_device_patterns: {}
  };
  
  // Analyze patterns
  for (const key of journeyKeys) {
    try {
      const journeyData = await redis(`get/${key}`);
      if (journeyData?.result) {
        const journey = JSON.parse(decodeURIComponent(journeyData.result));
        
        // Source sequence patterns
        if (journey.touchpoints && journey.touchpoints.length > 1) {
          const sequence = journey.touchpoints
            .filter(t => !t.is_conversion)
            .map(t => t.source)
            .join(' → ');
          
          if (sequence) {
            patterns.common_source_sequences[sequence] = (patterns.common_source_sequences[sequence] || 0) + 1;
          }
        }
        
        // Journey length patterns
        const lengthCategory = categorizeJourneyLength(journey.total_touchpoints);
        patterns.journey_length_patterns[lengthCategory] = (patterns.journey_length_patterns[lengthCategory] || 0) + 1;
        
        // Timing patterns
        const timingCategory = categorizeJourneyTiming(journey.journey_span_hours);
        patterns.conversion_timing_patterns[timingCategory] = (patterns.conversion_timing_patterns[timingCategory] || 0) + 1;
        
        // Cross-device patterns
        if (journey.cross_device_journey) {
          const devicePattern = `${journey.unique_device_fingerprints} devices`;
          patterns.cross_device_patterns[devicePattern] = (patterns.cross_device_patterns[devicePattern] || 0) + 1;
        }
      }
    } catch (parseError) {
      continue;
    }
  }
  
  return {
    success: true,
    pattern_analysis_timestamp: new Date().toISOString(),
    journeys_analyzed: journeyKeys.length,
    
    patterns: {
      most_common_source_sequences: Object.entries(patterns.common_source_sequences)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10)
        .map(([sequence, count]) => ({ sequence, count })),
        
      journey_length_distribution: patterns.journey_length_patterns,
      conversion_timing_distribution: patterns.conversion_timing_patterns,
      cross_device_distribution: patterns.cross_device_patterns
    },
    
    insights: {
      most_common_journey_length: Object.entries(patterns.journey_length_patterns)
        .sort(([,a], [,b]) => b - a)[0]?.[0],
      most_common_conversion_timing: Object.entries(patterns.conversion_timing_patterns)
        .sort(([,a], [,b]) => b - a)[0]?.[0],
      cross_device_usage_rate: ((Object.values(patterns.cross_device_patterns).reduce((a,b) => a+b, 0) / journeyKeys.length) * 100).toFixed(2)
    }
  };
}

// Helper functions for analysis

async function findJourneyKeys(redis, dateRangeDays) {
  const keys = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/1000`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const foundKeys = scanResult.result[1] || [];
      keys.push(...foundKeys);
      iterations++;
      
    } catch (scanError) {
      console.log(`⚠️ Journey key scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  return keys;
}

async function analyzeJourneys(redis, journeyKeys) {
  let totalJourneys = 0;
  let totalConversionValue = 0;
  let totalTouchpoints = 0;
  let singleTouchpointJourneys = 0;
  let crossSessionJourneys = 0;
  let crossDeviceJourneys = 0;
  let totalJourneySpanHours = 0;
  let maxTouchpoints = 0;
  let maxJourneySpanHours = 0;
  let highConfidenceAttributions = 0;
  
  const firstClickSources = {};
  const lastClickSources = {};
  const attributionMethods = {};
  const revenueByFirstClick = {};
  const revenueByLastClick = {};
  
  for (const key of journeyKeys) {
    try {
      const journeyData = await redis(`get/${key}`);
      if (journeyData?.result) {
        const journey = JSON.parse(decodeURIComponent(journeyData.result));
        
        totalJourneys++;
        totalConversionValue += journey.conversion_value || 0;
        totalTouchpoints += journey.total_touchpoints || 0;
        totalJourneySpanHours += journey.journey_span_hours || 0;
        
        if (journey.total_touchpoints === 1) singleTouchpointJourneys++;
        if (journey.cross_session_journey) crossSessionJourneys++;
        if (journey.cross_device_journey) crossDeviceJourneys++;
        if (journey.attribution_confidence_avg > 250) highConfidenceAttributions++;
        
        maxTouchpoints = Math.max(maxTouchpoints, journey.total_touchpoints || 0);
        maxJourneySpanHours = Math.max(maxJourneySpanHours, journey.journey_span_hours || 0);
        
        // Source analysis
        const firstSource = journey.first_click_source || 'unknown';
        const lastSource = journey.last_click_source || 'unknown';
        const revenue = journey.conversion_value || 0;
        
        firstClickSources[firstSource] = (firstClickSources[firstSource] || 0) + 1;
        lastClickSources[lastSource] = (lastClickSources[lastSource] || 0) + 1;
        revenueByFirstClick[firstSource] = (revenueByFirstClick[firstSource] || 0) + revenue;
        revenueByLastClick[lastSource] = (revenueByLastClick[lastSource] || 0) + revenue;
      }
    } catch (parseError) {
      continue;
    }
  }
  
  return {
    total_journeys: totalJourneys,
    total_conversion_value: totalConversionValue,
    avg_journey_value: totalConversionValue / totalJourneys,
    avg_touchpoints: totalTouchpoints / totalJourneys,
    total_touchpoints: totalTouchpoints,
    single_touchpoint_journeys: singleTouchpointJourneys,
    multi_touchpoint_journeys: totalJourneys - singleTouchpointJourneys,
    cross_session_journeys: crossSessionJourneys,
    cross_device_journeys: crossDeviceJourneys,
    avg_journey_span_hours: totalJourneySpanHours / totalJourneys,
    max_touchpoints: maxTouchpoints,
    max_journey_span_hours: maxJourneySpanHours,
    high_confidence_attributions: highConfidenceAttributions,
    first_click_sources: firstClickSources,
    last_click_sources: lastClickSources,
    attribution_methods: attributionMethods,
    revenue_by_first_click: revenueByFirstClick,
    revenue_by_last_click: revenueByLastClick,
    top_sources: Object.entries(revenueByFirstClick)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 10),
    value_distribution: categorizeValueDistribution(totalConversionValue, totalJourneys)
  };
}

function getSourcesOvervaluedByLastClick(comparison) {
  const overvalued = [];
  
  for (const source in comparison.last_click_attribution) {
    const lastClickRevenue = comparison.last_click_attribution[source]?.revenue || 0;
    const firstClickRevenue = comparison.first_click_attribution[source]?.revenue || 0;
    
    if (lastClickRevenue > firstClickRevenue) {
      overvalued.push({
        source,
        last_click_revenue: lastClickRevenue,
        first_click_revenue: firstClickRevenue,
        overvaluation: lastClickRevenue - firstClickRevenue
      });
    }
  }
  
  return overvalued.sort((a, b) => b.overvaluation - a.overvaluation).slice(0, 5);
}

function getSourcesUndervaluedByLastClick(comparison) {
  const undervalued = [];
  
  for (const source in comparison.first_click_attribution) {
    const firstClickRevenue = comparison.first_click_attribution[source]?.revenue || 0;
    const lastClickRevenue = comparison.last_click_attribution[source]?.revenue || 0;
    
    if (firstClickRevenue > lastClickRevenue) {
      undervalued.push({
        source,
        first_click_revenue: firstClickRevenue,
        last_click_revenue: lastClickRevenue,
        undervaluation: firstClickRevenue - lastClickRevenue
      });
    }
  }
  
  return undervalued.sort((a, b) => b.undervaluation - a.undervaluation).slice(0, 5);
}

function categorizeJourneyLength(touchpoints) {
  if (touchpoints === 1) return 'single_touchpoint';
  if (touchpoints <= 3) return 'short_2-3_touchpoints';
  if (touchpoints <= 5) return 'medium_4-5_touchpoints';
  if (touchpoints <= 10) return 'long_6-10_touchpoints';
  return 'very_long_10+_touchpoints';
}

function categorizeJourneyTiming(hours) {
  if (hours === 0) return 'immediate';
  if (hours <= 1) return 'within_1_hour';
  if (hours <= 24) return 'same_day';
  if (hours <= 72) return 'within_3_days';
  if (hours <= 168) return 'within_1_week';
  return 'longer_than_1_week';
}

function categorizeValueDistribution(totalValue, totalJourneys) {
  const avgValue = totalValue / totalJourneys;
  return {
    total_revenue: totalValue,
    average_order_value: avgValue,
    total_orders: totalJourneys
  };
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
