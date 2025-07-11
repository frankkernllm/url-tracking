// system-status.js - Check if automated extraction system is working
// Path: netlify/functions/system-status.js

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  try {
    console.log('🔍 SYSTEM STATUS: Checking extraction system health...');
    const startTime = Date.now();
    
    const redis = initializeRedis();
    
    // Check extraction metadata
    const extractionStatus = await checkExtractionMetadata(redis);
    
    // Check index availability
    const indexStatus = await checkIndexAvailability(redis);
    
    // Check scheduled extraction progress
    const scheduledStatus = await checkScheduledExtractionStatus(redis);
    
    // Test fast-analytics endpoint
    const fastAnalyticsStatus = await testFastAnalytics();
    
    const totalTime = Date.now() - startTime;
    
    // Determine overall system health
    const systemHealth = determineSystemHealth(extractionStatus, indexStatus, scheduledStatus, fastAnalyticsStatus);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        system_health: systemHealth.status,
        health_score: systemHealth.score,
        message: systemHealth.message,
        
        extraction_system: {
          pageview_extraction: extractionStatus.pageviews,
          conversion_extraction: extractionStatus.conversions,
          last_extraction: extractionStatus.last_extraction,
          extraction_frequency: extractionStatus.frequency
        },
        
        index_system: {
          pageview_indexes: indexStatus.pageview_indexes,
          conversion_indexes: indexStatus.conversion_indexes,
          index_freshness: indexStatus.freshness,
          total_data_coverage: indexStatus.coverage
        },
        
        scheduled_system: {
          auto_extraction_enabled: scheduledStatus.enabled,
          last_scheduled_run: scheduledStatus.last_run,
          next_scheduled_run: scheduledStatus.next_run,
          schedule_health: scheduledStatus.health
        },
        
        performance: {
          fast_analytics_working: fastAnalyticsStatus.working,
          response_time_ms: fastAnalyticsStatus.response_time_ms,
          data_freshness: fastAnalyticsStatus.data_freshness
        },
        
        recommendations: systemHealth.recommendations,
        
        quick_actions: {
          trigger_extraction: 'curl -X POST https://trackingojoy.netlify.app/.netlify/functions/complete-data-extractor',
          build_indexes: 'curl -X POST https://trackingojoy.netlify.app/.netlify/functions/build-indexes-complete',
          test_fast_analytics: 'curl "https://trackingojoy.netlify.app/.netlify/functions/fast-analytics?start_date=2025-07-11&end_date=2025-07-11"'
        },
        
        processing_time_ms: totalTime,
        timestamp: new Date().toISOString()
      })
    };
    
  } catch (error) {
    console.error('❌ System status check failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        system_health: 'CRITICAL_ERROR',
        error: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

// Check extraction metadata
async function checkExtractionMetadata(redis) {
  try {
    // Check pageview extraction metadata
    const pageviewMeta = await redis('get/pageview_extraction_metadata');
    
    // Check conversion extraction metadata (if exists)
    const conversionMeta = await redis('get/conversion_extraction_metadata');
    
    // Check scheduled extraction progress
    const scheduledProgress = await redis('get/continuous_processing_v2_progress');
    
    return {
      pageviews: {
        metadata_exists: !!pageviewMeta?.result,
        last_extraction: pageviewMeta?.result ? JSON.parse(pageviewMeta.result).extraction_timestamp : null,
        total_pageviews: pageviewMeta?.result ? JSON.parse(pageviewMeta.result).total_pageviews : 0,
        extraction_method: pageviewMeta?.result ? JSON.parse(pageviewMeta.result).extraction_method : null
      },
      conversions: {
        metadata_exists: !!conversionMeta?.result,
        last_extraction: conversionMeta?.result ? JSON.parse(conversionMeta.result).extraction_timestamp : null,
        total_conversions: conversionMeta?.result ? JSON.parse(conversionMeta.result).total_conversions : 0
      },
      last_extraction: pageviewMeta?.result ? JSON.parse(pageviewMeta.result).extraction_timestamp : null,
      frequency: 'Manual' // Since we need to set up automated scheduling
    };
  } catch (error) {
    return {
      pageviews: { metadata_exists: false, error: error.message },
      conversions: { metadata_exists: false, error: error.message },
      last_extraction: null,
      frequency: 'Unknown'
    };
  }
}

// Check index availability
async function checkIndexAvailability(redis) {
  try {
    // Count pageview IP indexes
    let pageviewIndexCount = 0;
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/pageview_index_ip:*/count/100`);
      if (scanResult?.result && scanResult.result[1]) {
        cursor = scanResult.result[0];
        pageviewIndexCount += scanResult.result[1].length;
      } else {
        break;
      }
    } while (cursor !== '0' && pageviewIndexCount < 1000);
    
    // Count conversion date indexes
    let conversionIndexCount = 0;
    cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversion_index_date:*/count/100`);
      if (scanResult?.result && scanResult.result[1]) {
        cursor = scanResult.result[0];
        conversionIndexCount += scanResult.result[1].length;
      } else {
        break;
      }
    } while (cursor !== '0' && conversionIndexCount < 100);
    
    return {
      pageview_indexes: {
        count: pageviewIndexCount,
        available: pageviewIndexCount > 0
      },
      conversion_indexes: {
        count: conversionIndexCount,
        available: conversionIndexCount > 0
      },
      freshness: 'Unknown', // Would need to check timestamps
      coverage: pageviewIndexCount > 0 ? 'Good' : 'None'
    };
  } catch (error) {
    return {
      pageview_indexes: { count: 0, available: false, error: error.message },
      conversion_indexes: { count: 0, available: false, error: error.message },
      freshness: 'Error',
      coverage: 'None'
    };
  }
}

// Check scheduled extraction status
async function checkScheduledExtractionStatus(redis) {
  try {
    // This would check if scheduled-extraction.js is running automatically
    // For now, we'll return that it needs to be set up
    return {
      enabled: false, // Needs manual setup
      last_run: null,
      next_run: 'Not scheduled',
      health: 'NEEDS_SETUP'
    };
  } catch (error) {
    return {
      enabled: false,
      last_run: null,
      next_run: 'Error',
      health: 'ERROR'
    };
  }
}

// Test fast-analytics endpoint
async function testFastAnalytics() {
  try {
    const testStart = Date.now();
    const testUrl = 'https://trackingojoy.netlify.app/.netlify/functions/fast-analytics?start_date=2025-07-11&end_date=2025-07-11&limit=10';
    
    const response = await fetch(testUrl, {
      signal: AbortSignal.timeout(5000) // 5 second timeout
    });
    
    const responseTime = Date.now() - testStart;
    
    if (response.ok) {
      const data = await response.json();
      return {
        working: true,
        response_time_ms: responseTime,
        data_freshness: 'Good',
        pageviews_found: data.total_page_views || 0,
        conversions_found: data.total_conversions || 0
      };
    } else {
      return {
        working: false,
        response_time_ms: responseTime,
        error: `HTTP ${response.status}`,
        data_freshness: 'Unknown'
      };
    }
  } catch (error) {
    return {
      working: false,
      response_time_ms: 0,
      error: error.message,
      data_freshness: 'Unknown'
    };
  }
}

// Determine overall system health
function determineSystemHealth(extraction, indexes, scheduled, performance) {
  let score = 0;
  const recommendations = [];
  
  // Check extraction system (25 points)
  if (extraction.pageviews.metadata_exists) score += 15;
  if (extraction.conversions.metadata_exists) score += 10;
  else recommendations.push('Run conversion extraction: complete-data-extractor.js');
  
  // Check index system (35 points)
  if (indexes.pageview_indexes.available) score += 20;
  else recommendations.push('Build pageview indexes: extract-pageviews-chunked.js + build-indexes.js');
  
  if (indexes.conversion_indexes.available) score += 15;
  else recommendations.push('Build conversion indexes: extract-conversions-chunked.js');
  
  // Check scheduled system (20 points)
  if (scheduled.enabled) score += 20;
  else recommendations.push('Set up automated extraction: Schedule scheduled-extraction.js to run hourly');
  
  // Check performance system (20 points)
  if (performance.working) score += 20;
  else recommendations.push('Fix fast-analytics endpoint or build missing indexes');
  
  let status, message;
  if (score >= 80) {
    status = 'HEALTHY';
    message = 'System is fully operational with automated extraction and fast analytics';
  } else if (score >= 60) {
    status = 'GOOD';
    message = 'System is mostly working but needs some improvements';
  } else if (score >= 40) {
    status = 'FAIR';
    message = 'System has basic functionality but missing key components';
  } else if (score >= 20) {
    status = 'POOR';
    message = 'System needs significant setup and improvements';
  } else {
    status = 'CRITICAL';
    message = 'System requires immediate attention and setup';
  }
  
  return { status, score, message, recommendations };
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
