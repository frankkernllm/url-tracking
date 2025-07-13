// attribution-model-calculator.js
// Multi-Touch Attribution Calculator - 5 Attribution Models
// Path: netlify/functions/attribution-model-calculator.js
// Purpose: Calculate attribution across multiple models for customer journeys

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

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid API key' })
    };
  }

  try {
    console.log('🎯 MULTI-TOUCH ATTRIBUTION CALCULATOR: Starting attribution analysis...');
    const startTime = Date.now();
    
    const redis = initializeRedis();
    const body = event.body ? JSON.parse(event.body) : {};
    
    const {
      journey_id,              // Single journey analysis
      bulk_analysis = false,   // Analyze all journeys
      date_range_days = 30,    // Date range for bulk analysis
      source_comparison = false, // Compare sources across models
      time_decay_half_life_hours = 168, // 7 days default half-life
      include_conversion_only = true,   // Include single-touchpoint journeys
      limit = 1000             // Limit for bulk analysis
    } = body;

    let results;

    if (journey_id) {
      // Single journey attribution analysis
      console.log(`🎯 Single journey analysis: ${journey_id}`);
      results = await analyzeSingleJourney(redis, journey_id, time_decay_half_life_hours);
      
    } else if (bulk_analysis) {
      // Bulk attribution analysis across multiple journeys
      console.log(`🎯 Bulk attribution analysis: ${date_range_days} days, limit ${limit}`);
      results = await analyzeBulkAttribution(redis, {
        date_range_days,
        source_comparison,
        time_decay_half_life_hours,
        include_conversion_only,
        limit
      });
      
    } else {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({
          error: 'Must specify either journey_id or bulk_analysis=true',
          usage: {
            single_journey: 'POST with {"journey_id": "journey_123"}',
            bulk_analysis: 'POST with {"bulk_analysis": true, "date_range_days": 30}',
            source_comparison: 'POST with {"bulk_analysis": true, "source_comparison": true}'
          }
        })
      };
    }

    const processingTime = Date.now() - startTime;
    console.log(`✅ Attribution analysis complete in ${processingTime}ms`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        attribution_results: results,
        processing_time_ms: processingTime,
        analysis_type: journey_id ? 'single_journey' : 'bulk_analysis',
        parameters: {
          time_decay_half_life_hours,
          include_conversion_only,
          date_range_days: bulk_analysis ? date_range_days : null,
          limit: bulk_analysis ? limit : null
        }
      })
    };

  } catch (error) {
    console.error('❌ Attribution calculation error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Attribution calculation failed',
        message: error.message
      })
    };
  }
};

// Analyze single customer journey
async function analyzeSingleJourney(redis, journeyId, timeDecayHalfLife) {
  console.log(`🔍 Loading journey: ${journeyId}`);
  
  try {
    const journeyKey = `customer_journey:${journeyId}`;
    const journeyData = await redis(`get/${journeyKey}`);
    
    if (!journeyData?.result) {
      throw new Error(`Journey ${journeyId} not found`);
    }
    
    const journey = JSON.parse(decodeURIComponent(journeyData.result));
    console.log(`📊 Journey loaded: ${journey.total_touchpoints} touchpoints, $${journey.conversion_value} value`);
    
    // Calculate all attribution models
    const attributionResults = calculateAllAttributionModels(journey, timeDecayHalfLife);
    
    return {
      journey_id: journeyId,
      customer_email: journey.customer_email,
      conversion_value: journey.conversion_value,
      total_touchpoints: journey.total_touchpoints,
      journey_span_hours: journey.journey_span_hours,
      touchpoint_details: journey.touchpoints.map(tp => ({
        timestamp: tp.timestamp,
        source: tp.source,
        campaign: tp.campaign,
        medium: tp.medium,
        landing_page: tp.landing_page,
        touchpoint_position: tp.touchpoint_position,
        is_conversion: tp.is_conversion || false
      })),
      attribution_models: attributionResults,
      insights: generateJourneyInsights(journey, attributionResults)
    };
    
  } catch (error) {
    console.error(`❌ Error analyzing journey ${journeyId}:`, error);
    throw error;
  }
}

// Bulk attribution analysis across multiple journeys
async function analyzeBulkAttribution(redis, options) {
  const { date_range_days, source_comparison, time_decay_half_life_hours, include_conversion_only, limit } = options;
  
  console.log(`🔍 Loading journeys for bulk analysis...`);
  
  // Load journeys from Redis
  const journeys = await loadJourneysForAttribution(redis, date_range_days, limit);
  console.log(`📊 Loaded ${journeys.length} journeys for attribution analysis`);
  
  if (journeys.length === 0) {
    return {
      total_journeys: 0,
      message: 'No journeys found for the specified criteria'
    };
  }
  
  // Filter by touchpoint criteria
  let filteredJourneys = journeys;
  if (!include_conversion_only) {
    filteredJourneys = journeys.filter(j => j.total_touchpoints > 1);
    console.log(`📊 Filtered to ${filteredJourneys.length} multi-touchpoint journeys`);
  }
  
  // Calculate attribution for all journeys
  const allAttributionResults = [];
  const sourceAttribution = {}; // Track attribution by source across all models
  let totalConversionValue = 0;
  
  for (const journey of filteredJourneys) {
    try {
      const attributionResult = calculateAllAttributionModels(journey, time_decay_half_life_hours);
      
      allAttributionResults.push({
        journey_id: journey.journey_id,
        customer_email: journey.customer_email,
        conversion_value: journey.conversion_value,
        total_touchpoints: journey.total_touchpoints,
        attribution_models: attributionResult
      });
      
      totalConversionValue += journey.conversion_value;
      
      // Aggregate attribution by source if requested
      if (source_comparison) {
        aggregateSourceAttribution(sourceAttribution, attributionResult, journey.conversion_value);
      }
      
    } catch (journeyError) {
      console.warn(`⚠️ Error processing journey ${journey.journey_id}:`, journeyError.message);
    }
  }
  
  // Calculate aggregated metrics
  const aggregatedResults = calculateAggregatedAttribution(allAttributionResults);
  
  const results = {
    total_journeys: filteredJourneys.length,
    total_conversion_value: totalConversionValue,
    average_conversion_value: totalConversionValue / filteredJourneys.length,
    attribution_summary: aggregatedResults,
    model_comparison: generateModelComparison(aggregatedResults),
  };
  
  // Add source comparison if requested
  if (source_comparison && Object.keys(sourceAttribution).length > 0) {
    results.source_attribution_comparison = generateSourceComparison(sourceAttribution, totalConversionValue);
  }
  
  // Add individual journey results for smaller datasets
  if (filteredJourneys.length <= 50) {
    results.individual_journey_results = allAttributionResults;
  }
  
  return results;
}

// Core attribution calculation function
function calculateAllAttributionModels(journey, timeDecayHalfLife = 168) {
  const touchpoints = journey.touchpoints || [];
  const conversionValue = journey.conversion_value || 0;
  
  // Filter out the conversion point itself for attribution (keep only pageview touchpoints)
  const attributabletouchpoints = touchpoints.filter(tp => !tp.is_conversion);
  
  if (attributabletouchpoints.length === 0) {
    // Conversion-only journey - attribute to direct/unknown
    return {
      first_click: [{ source: 'direct', credit: conversionValue }],
      last_click: [{ source: 'direct', credit: conversionValue }],
      linear: [{ source: 'direct', credit: conversionValue }],
      time_decay: [{ source: 'direct', credit: conversionValue }],
      position_based: [{ source: 'direct', credit: conversionValue }],
      model_type: 'conversion_only'
    };
  }
  
  return {
    first_click: calculateFirstClickAttribution(attributableouchpoints, conversionValue),
    last_click: calculateLastClickAttribution(attributableouchpoints, conversionValue),
    linear: calculateLinearAttribution(attributableouchpoints, conversionValue),
    time_decay: calculateTimeDecayAttribution(attributableouchpoints, conversionValue, timeDecayHalfLife),
    position_based: calculatePositionBasedAttribution(attributableouchpoints, conversionValue),
    model_type: 'multi_touchpoint',
    touchpoint_count: attributableouchpoints.length
  };
}

// First-Click Attribution: 100% credit to first touchpoint
function calculateFirstClickAttribution(touchpoints, conversionValue) {
  if (touchpoints.length === 0) return [];
  
  const firstTouchpoint = touchpoints[0];
  return [{
    source: firstTouchpoint.source || 'unknown',
    campaign: firstTouchpoint.campaign,
    medium: firstTouchpoint.medium,
    timestamp: firstTouchpoint.timestamp,
    touchpoint_position: firstTouchpoint.touchpoint_position,
    credit: conversionValue,
    credit_percentage: 100
  }];
}

// Last-Click Attribution: 100% credit to last touchpoint
function calculateLastClickAttribution(touchpoints, conversionValue) {
  if (touchpoints.length === 0) return [];
  
  const lastTouchpoint = touchpoints[touchpoints.length - 1];
  return [{
    source: lastTouchpoint.source || 'unknown',
    campaign: lastTouchpoint.campaign,
    medium: lastTouchpoint.medium,
    timestamp: lastTouchpoint.timestamp,
    touchpoint_position: lastTouchpoint.touchpoint_position,
    credit: conversionValue,
    credit_percentage: 100
  }];
}

// Linear Attribution: Equal credit across all touchpoints
function calculateLinearAttribution(touchpoints, conversionValue) {
  if (touchpoints.length === 0) return [];
  
  const creditPerTouchpoint = conversionValue / touchpoints.length;
  const creditPercentage = 100 / touchpoints.length;
  
  return touchpoints.map(tp => ({
    source: tp.source || 'unknown',
    campaign: tp.campaign,
    medium: tp.medium,
    timestamp: tp.timestamp,
    touchpoint_position: tp.touchpoint_position,
    credit: creditPerTouchpoint,
    credit_percentage: creditPercentage
  }));
}

// Time-Decay Attribution: More credit to recent touchpoints
function calculateTimeDecayAttribution(touchpoints, conversionValue, halfLifeHours) {
  if (touchpoints.length === 0) return [];
  
  // Get conversion time (from the journey end or use current time)
  const conversionTime = new Date(touchpoints[touchpoints.length - 1].timestamp).getTime();
  
  // Calculate decay weights for each touchpoint
  const weights = touchpoints.map(tp => {
    const touchpointTime = new Date(tp.timestamp).getTime();
    const hoursBeforeConversion = (conversionTime - touchpointTime) / (1000 * 60 * 60);
    
    // Exponential decay: weight = 2^(-hours_before_conversion / half_life)
    const weight = Math.pow(2, -hoursBeforeConversion / halfLifeHours);
    
    return { touchpoint: tp, weight };
  });
  
  // Calculate total weight for normalization
  const totalWeight = weights.reduce((sum, w) => sum + w.weight, 0);
  
  // Distribute credit based on weights
  return weights.map(w => {
    const creditPercentage = (w.weight / totalWeight) * 100;
    const credit = (w.weight / totalWeight) * conversionValue;
    
    return {
      source: w.touchpoint.source || 'unknown',
      campaign: w.touchpoint.campaign,
      medium: w.touchpoint.medium,
      timestamp: w.touchpoint.timestamp,
      touchpoint_position: w.touchpoint.touchpoint_position,
      credit: credit,
      credit_percentage: creditPercentage,
      decay_weight: w.weight,
      hours_before_conversion: (conversionTime - new Date(w.touchpoint.timestamp).getTime()) / (1000 * 60 * 60)
    };
  });
}

// Position-Based Attribution: 40% first, 40% last, 20% middle
function calculatePositionBasedAttribution(touchpoints, conversionValue) {
  if (touchpoints.length === 0) return [];
  
  if (touchpoints.length === 1) {
    // Single touchpoint gets 100%
    return [{
      source: touchpoints[0].source || 'unknown',
      campaign: touchpoints[0].campaign,
      medium: touchpoints[0].medium,
      timestamp: touchpoints[0].timestamp,
      touchpoint_position: touchpoints[0].touchpoint_position,
      credit: conversionValue,
      credit_percentage: 100,
      position_role: 'only'
    }];
  }
  
  if (touchpoints.length === 2) {
    // Two touchpoints: 50% each (simplified)
    return touchpoints.map((tp, index) => ({
      source: tp.source || 'unknown',
      campaign: tp.campaign,
      medium: tp.medium,
      timestamp: tp.timestamp,
      touchpoint_position: tp.touchpoint_position,
      credit: conversionValue * 0.5,
      credit_percentage: 50,
      position_role: index === 0 ? 'first' : 'last'
    }));
  }
  
  // Three or more touchpoints: 40% first, 40% last, 20% distributed among middle
  const firstCredit = conversionValue * 0.4;
  const lastCredit = conversionValue * 0.4;
  const middleCredit = conversionValue * 0.2;
  const middleTouchpoints = touchpoints.slice(1, -1);
  const creditPerMiddleTouchpoint = middleTouchpoints.length > 0 ? middleCredit / middleTouchpoints.length : 0;
  
  const results = [];
  
  // First touchpoint (40%)
  results.push({
    source: touchpoints[0].source || 'unknown',
    campaign: touchpoints[0].campaign,
    medium: touchpoints[0].medium,
    timestamp: touchpoints[0].timestamp,
    touchpoint_position: touchpoints[0].touchpoint_position,
    credit: firstCredit,
    credit_percentage: 40,
    position_role: 'first'
  });
  
  // Middle touchpoints (20% total, distributed equally)
  middleTouchpoints.forEach(tp => {
    const middlePercentage = (creditPerMiddleTouchpoint / conversionValue) * 100;
    results.push({
      source: tp.source || 'unknown',
      campaign: tp.campaign,
      medium: tp.medium,
      timestamp: tp.timestamp,
      touchpoint_position: tp.touchpoint_position,
      credit: creditPerMiddleTouchpoint,
      credit_percentage: middlePercentage,
      position_role: 'middle'
    });
  });
  
  // Last touchpoint (40%)
  results.push({
    source: touchpoints[touchpoints.length - 1].source || 'unknown',
    campaign: touchpoints[touchpoints.length - 1].campaign,
    medium: touchpoints[touchpoints.length - 1].medium,
    timestamp: touchpoints[touchpoints.length - 1].timestamp,
    touchpoint_position: touchpoints[touchpoints.length - 1].touchpoint_position,
    credit: lastCredit,
    credit_percentage: 40,
    position_role: 'last'
  });
  
  return results;
}

// Load journeys for attribution analysis
async function loadJourneysForAttribution(redis, dateRangeDays, limit) {
  console.log(`🔍 Scanning for customer journeys (${dateRangeDays} days, limit ${limit})...`);
  
  const journeys = [];
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - dateRangeDays);
  
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Load journey data in batches
      const batchSize = 50;
      for (let i = 0; i < keys.length && journeys.length < limit; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              // Filter by date range
              const conversionDate = new Date(journey.conversion_timestamp);
              if (conversionDate >= cutoffDate && journey.conversion_value > 0) {
                return journey;
              }
            }
          } catch (parseError) {
            // Skip invalid journey data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validJourneys = batchResults.filter(j => j !== null);
        journeys.push(...validJourneys);
        
        if (journeys.length >= limit) break;
      }
      
    } catch (scanError) {
      console.log(`⚠️ Journey scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations && journeys.length < limit);
  
  // Sort by conversion timestamp (most recent first)
  journeys.sort((a, b) => new Date(b.conversion_timestamp) - new Date(a.conversion_timestamp));
  
  return journeys.slice(0, limit);
}

// Aggregate source attribution across all models
function aggregateSourceAttribution(sourceAttribution, attributionResult, conversionValue) {
  const models = ['first_click', 'last_click', 'linear', 'time_decay', 'position_based'];
  
  models.forEach(model => {
    const modelResults = attributionResult[model] || [];
    
    modelResults.forEach(result => {
      const source = result.source || 'unknown';
      
      if (!sourceAttribution[source]) {
        sourceAttribution[source] = {
          first_click: 0,
          last_click: 0,
          linear: 0,
          time_decay: 0,
          position_based: 0
        };
      }
      
      sourceAttribution[source][model] += result.credit || 0;
    });
  });
}

// Calculate aggregated attribution across all journeys
function calculateAggregatedAttribution(allResults) {
  const aggregated = {
    first_click: 0,
    last_click: 0,
    linear: 0,
    time_decay: 0,
    position_based: 0
  };
  
  allResults.forEach(result => {
    const models = result.attribution_models;
    
    // Sum up total credit for each model
    aggregated.first_click += sumModelCredit(models.first_click);
    aggregated.last_click += sumModelCredit(models.last_click);
    aggregated.linear += sumModelCredit(models.linear);
    aggregated.time_decay += sumModelCredit(models.time_decay);
    aggregated.position_based += sumModelCredit(models.position_based);
  });
  
  return aggregated;
}

// Helper function to sum credit from a model result
function sumModelCredit(modelResults) {
  if (!Array.isArray(modelResults)) return 0;
  return modelResults.reduce((sum, result) => sum + (result.credit || 0), 0);
}

// Generate model comparison insights
function generateModelComparison(aggregatedResults) {
  const models = Object.keys(aggregatedResults);
  const totalValue = Math.max(...Object.values(aggregatedResults));
  
  return models.map(model => ({
    model_name: model,
    total_attributed_value: aggregatedResults[model],
    percentage_of_max: ((aggregatedResults[model] / totalValue) * 100).toFixed(2)
  })).sort((a, b) => b.total_attributed_value - a.total_attributed_value);
}

// Generate source comparison across models
function generateSourceComparison(sourceAttribution, totalConversionValue) {
  const sources = Object.keys(sourceAttribution);
  
  return sources.map(source => {
    const sourceData = sourceAttribution[source];
    const models = Object.keys(sourceData);
    
    const comparison = {
      source: source,
      attribution_by_model: {}
    };
    
    models.forEach(model => {
      comparison.attribution_by_model[model] = {
        credited_value: sourceData[model],
        percentage_of_total: ((sourceData[model] / totalConversionValue) * 100).toFixed(2)
      };
    });
    
    // Calculate variance across models for this source
    const values = models.map(model => sourceData[model]);
    const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
    const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
    comparison.attribution_variance = Math.sqrt(variance);
    comparison.most_favorable_model = models.reduce((max, model) => 
      sourceData[model] > sourceData[max] ? model : max
    );
    comparison.least_favorable_model = models.reduce((min, model) => 
      sourceData[model] < sourceData[min] ? model : min
    );
    
    return comparison;
  }).sort((a, b) => b.attribution_by_model.linear.credited_value - a.attribution_by_model.linear.credited_value);
}

// Generate insights for individual journey
function generateJourneyInsights(journey, attributionResults) {
  const insights = [];
  
  // Journey type insight
  if (journey.total_touchpoints === 1) {
    insights.push("Single-touchpoint journey - all models attribute to same source");
  } else {
    insights.push(`Multi-touchpoint journey with ${journey.total_touchpoints - 1} attribution points`);
  }
  
  // Time span insight
  if (journey.journey_span_hours < 1) {
    insights.push("Quick conversion (< 1 hour) - time decay has minimal impact");
  } else if (journey.journey_span_hours > 168) {
    insights.push("Extended journey (> 1 week) - first-click and time decay differ significantly");
  }
  
  // Cross-session insight
  if (journey.cross_session_journey) {
    insights.push("Cross-session journey - indicates considered purchase decision");
  }
  
  // Model difference insight
  const firstClickValue = sumModelCredit(attributionResults.first_click);
  const lastClickValue = sumModelCredit(attributionResults.last_click);
  
  if (firstClickValue !== lastClickValue) {
    insights.push("First-click and last-click sources differ - multi-touch attribution provides different insights");
  }
  
  return insights;
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
