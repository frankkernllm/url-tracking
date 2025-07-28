// Query Conversion Indexes V2 - Enhanced Data Sources
// Path: netlify/functions/query-conversion-indexes-v2.js
// Purpose: Query the V2 conversion indexes created by extract-conversion-data-v2.js
//
// V2 ENHANCEMENTS:
// - Queries V2 indexes with enhanced email validation
// - Exposes V2 metadata (data sources, index types, extraction methods)
// - Enhanced filtering results (no invalid emails)
// - Better error handling and data quality insights
// - Support for verification recovery data tracking

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    // Parse query parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const queryParams = event.queryStringParameters || {};
    
    // Extract query parameters
    const query = {
      // Query type: email, ip, session, date, summary, or global_index
      type: body.type || queryParams.type || 'summary',
      
      // Specific lookup values
      email: body.email || queryParams.email,
      ip: body.ip || queryParams.ip,
      session_id: body.session_id || queryParams.session_id,
      date: body.date || queryParams.date, // Format: YYYY-MM-DD
      
      // NEW V2: Global conversion index lookup
      conversion_index: parseInt(body.conversion_index || queryParams.conversion_index || '-1'),
      
      // Options
      limit: parseInt(body.limit || queryParams.limit || '50'),
      include_raw_data: body.include_raw_data === 'true' || queryParams.include_raw_data === 'true',
      
      // NEW V2: Enhanced options
      include_v2_metadata: body.include_v2_metadata !== 'false' && queryParams.include_v2_metadata !== 'false', // Default true
      show_data_sources: body.show_data_sources === 'true' || queryParams.show_data_sources === 'true'
    };
    
    console.log('🔍 V2 CONVERSION INDEX QUERY starting with parameters:', query);
    
    let result;
    
    switch (query.type) {
      case 'email':
        result = await queryV2EmailIndex(redis, query);
        break;
      case 'ip':
        result = await queryV2IPIndex(redis, query);
        break;
      case 'session':
        result = await queryV2SessionIndex(redis, query);
        break;
      case 'date':
        result = await queryV2DateIndex(redis, query);
        break;
      case 'global_index':
        result = await queryV2GlobalIndex(redis, query);
        break;
      case 'summary':
      default:
        result = await getV2IndexSummary(redis, query);
        break;
    }
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ V2 Conversion index query completed in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        query_version: 'v2_enhanced',
        query_type: query.type,
        query_parameters: query,
        result: result,
        v2_features: {
          enhanced_email_validation: true,
          verification_recovery_tracking: true,
          data_source_transparency: query.show_data_sources,
          global_index_support: true
        },
        processing_time_ms: totalTime
      })
    };
    
  } catch (error) {
    console.error('❌ V2 Conversion index query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'V2 Conversion index query failed', 
        message: error.message 
      })
    };
  }
};

// Query V2 email-specific conversion index
async function queryV2EmailIndex(redis, query) {
  if (!query.email) {
    return {
      error: 'Email parameter required for email query',
      example: 'Add "email": "customer@example.com" to your request',
      v2_note: 'V2 indexes only contain validated email addresses'
    };
  }
  
  try {
    const emailKey = `conversion_index_v2_email:${encodeURIComponent(query.email.toLowerCase())}`;
    console.log(`🔍 Querying V2 email index: ${emailKey}`);
    
    const result = await redis(`get/${emailKey}`);
    
    if (!result?.result) {
      return {
        email: query.email,
        found: false,
        message: 'No conversions found for this email address in V2 indexes',
        suggestion: 'Check if email exists with summary query first',
        v2_note: 'V2 indexes exclude conversions with invalid/missing emails'
      };
    }
    
    const emailData = JSON.parse(decodeURIComponent(result.result));
    
    // Limit conversions if requested
    let conversions = emailData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    const response = {
      email: query.email,
      found: true,
      summary: {
        conversion_count: emailData.conversion_count,
        total_revenue: emailData.total_revenue,
        latest_conversion: emailData.latest_conversion,
        conversions_returned: conversions.length
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        order_total: conv.order_total,
        conversion_ip: conv.conversion_ip,
        primary_ip: conv.primary_ip,
        unique_ips_count: conv.unique_ips?.length || 0,
        ssid: conv.ssid,
        landing_page: conv.landing_page,
        source: conv.source,
        order_id: conv.order_id
      }))
    };
    
    // Add V2 metadata if requested
    if (query.include_v2_metadata) {
      response.v2_metadata = {
        index_type: emailData.index_type || 'email_conversions_v2',
        created_at: emailData.created_at,
        email_validation: 'v2_enhanced',
        data_quality: 'high_confidence'
      };
    }
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 Email index query error:', error);
    return {
      email: query.email,
      error: 'Failed to query V2 email index',
      message: error.message
    };
  }
}

// Query V2 IP-specific conversion index
async function queryV2IPIndex(redis, query) {
  if (!query.ip) {
    return {
      error: 'IP parameter required for IP query',
      example: 'Add "ip": "192.168.1.1" to your request'
    };
  }
  
  try {
    const encodedIP = query.ip.replace(/:/g, '_').replace(/\./g, '_');
    const ipKey = `conversion_index_v2_ip:${encodedIP}`;
    console.log(`🔍 Querying V2 IP index: ${ipKey}`);
    
    const result = await redis(`get/${ipKey}`);
    
    if (!result?.result) {
      return {
        ip: query.ip,
        found: false,
        message: 'No conversions found for this IP address in V2 indexes'
      };
    }
    
    const ipData = JSON.parse(decodeURIComponent(result.result));
    
    let conversions = ipData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    const response = {
      ip: query.ip,
      found: true,
      summary: {
        conversion_count: ipData.conversion_count,
        unique_emails: ipData.unique_emails,
        total_revenue: ipData.total_revenue,
        latest_conversion: ipData.latest_conversion,
        conversions_returned: conversions.length,
        is_target_ip: query.ip === '42.61.210.120' // Special handling for target IP
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        email: conv.email,
        order_total: conv.order_total,
        ssid: conv.ssid,
        landing_page: conv.landing_page,
        source: conv.source,
        order_id: conv.order_id
      }))
    };
    
    // Add V2 metadata if requested
    if (query.include_v2_metadata) {
      response.v2_metadata = {
        index_type: ipData.index_type || 'ip_conversions_v2',
        created_at: ipData.created_at,
        target_ip_detection: query.ip === '42.61.210.120'
      };
    }
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 IP index query error:', error);
    return {
      ip: query.ip,
      error: 'Failed to query V2 IP index',
      message: error.message
    };
  }
}

// Query V2 session-specific conversion index
async function queryV2SessionIndex(redis, query) {
  if (!query.session_id) {
    return {
      error: 'Session ID parameter required for session query',
      example: 'Add "session_id": "session_123" to your request'
    };
  }
  
  try {
    const sessionKey = `conversion_index_v2_session:${query.session_id}`;
    console.log(`🔍 Querying V2 session index: ${sessionKey}`);
    
    const result = await redis(`get/${sessionKey}`);
    
    if (!result?.result) {
      return {
        session_id: query.session_id,
        found: false,
        message: 'No conversions found for this session ID in V2 indexes'
      };
    }
    
    const sessionData = JSON.parse(decodeURIComponent(result.result));
    
    let conversions = sessionData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    const response = {
      session_id: query.session_id,
      found: true,
      summary: {
        conversion_count: sessionData.conversion_count,
        unique_emails: sessionData.unique_emails,
        total_revenue: sessionData.total_revenue,
        latest_conversion: sessionData.latest_conversion,
        conversions_returned: conversions.length
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        email: conv.email,
        order_total: conv.order_total,
        conversion_ip: conv.conversion_ip,
        primary_ip: conv.primary_ip,
        landing_page: conv.landing_page,
        source: conv.source,
        order_id: conv.order_id
      }))
    };
    
    // Add V2 metadata if requested
    if (query.include_v2_metadata) {
      response.v2_metadata = {
        index_type: sessionData.index_type || 'session_conversions_v2',
        created_at: sessionData.created_at
      };
    }
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 Session index query error:', error);
    return {
      session_id: query.session_id,
      error: 'Failed to query V2 session index',
      message: error.message
    };
  }
}

// Query V2 date-specific conversion index
async function queryV2DateIndex(redis, query) {
  if (!query.date) {
    // Default to today if no date specified
    query.date = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
  }
  
  try {
    const dateKey = `conversion_index_v2_date:${query.date}`;
    console.log(`🔍 Querying V2 date index: ${dateKey}`);
    
    const result = await redis(`get/${dateKey}`);
    
    if (!result?.result) {
      return {
        date: query.date,
        found: false,
        message: 'No conversions found for this date in V2 indexes',
        suggestion: 'Try dates between 2025-06-08 and 2025-07-27',
        v2_note: 'V2 indexes only contain conversions with valid emails'
      };
    }
    
    const dateData = JSON.parse(decodeURIComponent(result.result));
    
    let conversions = dateData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    const response = {
      date: query.date,
      found: true,
      summary: {
        conversion_count: dateData.conversion_count,
        unique_emails: dateData.unique_emails?.length || 0,
        unique_ips: dateData.unique_ips?.length || 0,
        total_revenue: dateData.total_revenue,
        conversions_returned: conversions.length
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        email: conv.email,
        order_total: conv.order_total,
        conversion_ip: conv.conversion_ip,
        primary_ip: conv.primary_ip,
        unique_ips_count: conv.unique_ips?.length || 0,
        ssid: conv.ssid,
        landing_page: conv.landing_page,
        source: conv.source,
        order_id: conv.order_id
      }))
    };
    
    // Add V2 metadata if requested
    if (query.include_v2_metadata) {
      response.v2_metadata = {
        index_type: dateData.index_type || 'date_conversions_v2',
        created_at: dateData.created_at,
        email_validation: 'v2_enhanced',
        target_ip_conversions: conversions.filter(conv => 
          conv.conversion_ip === '42.61.210.120' || 
          conv.primary_ip === '42.61.210.120' ||
          (conv.unique_ips && conv.unique_ips.includes('42.61.210.120'))
        ).length
      };
    }
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 Date index query error:', error);
    return {
      date: query.date,
      error: 'Failed to query V2 date index',
      message: error.message
    };
  }
}

// NEW V2: Query by global conversion index (like multi-touch-attribution-v2.js)
async function queryV2GlobalIndex(redis, query) {
  if (query.conversion_index < 0) {
    return {
      error: 'Conversion index parameter required for global index query',
      example: 'Add "conversion_index": 0 to your request (0 = most recent)',
      note: 'Global index allows browsing all conversions by recency'
    };
  }
  
  try {
    console.log(`🔍 V2 Global conversion lookup by index: ${query.conversion_index}`);
    
    // Scan for all V2 email conversion indexes to build global list
    let allConversions = [];
    let cursor = '0';
    let scannedIndexes = 0;
    
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversion_index_v2_email:*/count/100`);
      
      if (scanResult?.result && Array.isArray(scanResult.result) && scanResult.result.length >= 2) {
        cursor = scanResult.result[0];
        const emailIndexKeys = scanResult.result[1] || [];
        
        // Get conversions from each email index
        for (const emailIndexKey of emailIndexKeys) {
          try {
            const indexResult = await redis(`get/${emailIndexKey}`, 2000);
            if (indexResult?.result) {
              const emailConversions = JSON.parse(decodeURIComponent(indexResult.result));
              if (emailConversions.conversions) {
                allConversions.push(...emailConversions.conversions);
              }
            }
          } catch (error) {
            console.log(`⚠️ Error reading email index: ${error.message}`);
          }
        }
        
        scannedIndexes += emailIndexKeys.length;
      } else {
        cursor = '0';
      }
      
      // Safety limit
      if (scannedIndexes >= 1000) {
        console.log(`🛑 Reached safety limit of 1000 email indexes`);
        break;
      }
      
    } while (cursor !== '0');
    
    // Sort by timestamp (newest first) and select by index
    allConversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`📊 Found ${allConversions.length} total V2 conversions globally`);
    
    if (query.conversion_index >= allConversions.length) {
      return {
        conversion_index: query.conversion_index,
        found: false,
        message: `Global conversion index ${query.conversion_index} out of range`,
        available_range: `0-${allConversions.length - 1}`,
        total_conversions: allConversions.length
      };
    }
    
    const selectedConversion = allConversions[query.conversion_index];
    
    const response = {
      conversion_index: query.conversion_index,
      found: true,
      total_conversions_available: allConversions.length,
      selected_conversion: query.include_raw_data ? selectedConversion : {
        timestamp: selectedConversion.timestamp,
        email: selectedConversion.email,
        order_total: selectedConversion.order_total,
        conversion_ip: selectedConversion.conversion_ip,
        primary_ip: selectedConversion.primary_ip,
        unique_ips_count: selectedConversion.unique_ips?.length || 0,
        ssid: selectedConversion.ssid,
        landing_page: selectedConversion.landing_page,
        source: selectedConversion.source,
        order_id: selectedConversion.order_id
      }
    };
    
    // Add V2 metadata if requested
    if (query.include_v2_metadata) {
      response.v2_metadata = {
        selection_method: 'global_index_scan',
        sort_order: 'newest_first',
        email_validation: 'v2_enhanced',
        extracted_at: selectedConversion.extracted_at,
        is_target_ip: selectedConversion.conversion_ip === '42.61.210.120' || 
                      selectedConversion.primary_ip === '42.61.210.120' ||
                      (selectedConversion.unique_ips && selectedConversion.unique_ips.includes('42.61.210.120'))
      };
    }
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 Global index query error:', error);
    return {
      conversion_index: query.conversion_index,
      error: 'Failed to query V2 global index',
      message: error.message
    };
  }
}

// Get V2 enhanced summary of all conversion indexes
async function getV2IndexSummary(redis, query) {
  console.log('📊 Getting V2 conversion index summary...');
  
  try {
    const summary = {
      v2_extraction_summary: {
        data_sources: ['enhanced_extraction_v2', 'verification_recovery_v2'],
        email_validation: 'v2_enhanced',
        invalid_emails_filtered: true,
        target_ip_tracking: '42.61.210.120',
        extraction_method: 'conversion_extraction_indexing_v2'
      },
      index_types: [
        {
          type: "email",
          description: "Query by email address (V2 validated emails only)",
          key_pattern: "conversion_index_v2_email:*",
          example_query: {
            type: "email",
            email: "customer@example.com"
          }
        },
        {
          type: "ip", 
          description: "Query by IP address (includes target IP detection)",
          key_pattern: "conversion_index_v2_ip:*",
          example_query: {
            type: "ip",
            ip: "42.61.210.120"
          }
        },
        {
          type: "session",
          description: "Query by session ID",
          key_pattern: "conversion_index_v2_session:*",
          example_query: {
            type: "session",
            session_id: "session_123"
          }
        },
        {
          type: "date",
          description: "Query by date (YYYY-MM-DD)",
          key_pattern: "conversion_index_v2_date:*",
          example_query: {
            type: "date",
            date: "2025-07-27"
          }
        },
        {
          type: "global_index",
          description: "Browse all conversions by global index (newest first)",
          example_query: {
            type: "global_index",
            conversion_index: 0
          }
        }
      ],
      sample_queries: [
        "Today's conversions: type=date&date=2025-07-27",
        "July 1st conversions: type=date&date=2025-07-01", 
        "Email lookup: type=email&email=customer@example.com",
        "Target IP lookup: type=ip&ip=42.61.210.120",
        "Most recent conversion: type=global_index&conversion_index=0",
        "5th most recent: type=global_index&conversion_index=4"
      ],
      v2_features: {
        enhanced_email_validation: "Only valid email formats included",
        verification_recovery_data: "Includes missed conversions from verification passes",
        target_ip_detection: "Special handling for 42.61.210.120",
        global_index_support: "Browse all conversions by recency",
        data_source_transparency: "Track extraction method for each conversion"
      }
    };
    
    // Try to get sample data to verify V2 indexes exist
    const todayKey = `conversion_index_v2_date:${new Date().toISOString().split('T')[0]}`;
    const todayResult = await redis(`get/${todayKey}`);
    
    if (todayResult?.result) {
      const todayData = JSON.parse(decodeURIComponent(todayResult.result));
      summary.today_sample = {
        date: new Date().toISOString().split('T')[0],
        conversion_count: todayData.conversion_count,
        total_revenue: todayData.total_revenue,
        unique_emails: todayData.unique_emails?.length || 0,
        index_type: todayData.index_type,
        created_at: todayData.created_at
      };
    } else {
      summary.today_sample = {
        message: "No V2 conversions found for today",
        suggestion: "Try querying a recent date or check if extract-conversion-data-v2.js completed"
      };
    }
    
    // Sample target IP lookup
    try {
      const targetIPKey = `conversion_index_v2_ip:42_61_210_120`;
      const targetIPResult = await redis(`get/${targetIPKey}`);
      
      if (targetIPResult?.result) {
        const targetIPData = JSON.parse(decodeURIComponent(targetIPResult.result));
        summary.target_ip_sample = {
          ip: '42.61.210.120',
          conversion_count: targetIPData.conversion_count,
          total_revenue: targetIPData.total_revenue,
          unique_emails: targetIPData.unique_emails?.length || 0,
          note: 'Special target IP tracked in V2 system'
        };
      }
    } catch (error) {
      console.log('⚠️ Could not check target IP sample:', error.message);
    }
    
    return summary;
    
  } catch (error) {
    console.error('❌ V2 Summary generation error:', error);
    return {
      error: 'Failed to generate V2 summary',
      message: error.message
    };
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
