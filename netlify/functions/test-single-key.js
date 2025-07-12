// Create this as: netlify/functions/test-single-key.js
// Quick test to see if we can extract data from one known key

exports.handler = async (event, context) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const redis = (command) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${command}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    // Test with one of the known keys from the diagnostic
    const testKey = 'attribution_1.127.108.160_1750140065941';
    console.log(`🧪 Testing extraction from key: ${testKey}`);
    
    const data = await redis(`get/${testKey}`);
    console.log('Raw data result:', data);
    
    if (!data?.result) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No data found for test key',
          test_key: testKey
        })
      };
    }
    
    // Try both parsing methods
    let parsed = null;
    let parseMethod = 'unknown';
    
    try {
      // Method 1: Direct JSON parse
      parsed = JSON.parse(data.result);
      parseMethod = 'direct_json';
      console.log('✅ Direct JSON parsing worked');
    } catch (directError) {
      try {
        // Method 2: URL decode then JSON parse
        parsed = JSON.parse(decodeURIComponent(data.result));
        parseMethod = 'url_decoded_json';
        console.log('✅ URL-decoded JSON parsing worked');
      } catch (decodedError) {
        console.log('❌ Both parsing methods failed');
        return {
          statusCode: 200,
          headers,
          body: JSON.stringify({
            success: false,
            message: 'JSON parsing failed with both methods',
            direct_error: directError.message,
            decoded_error: decodedError.message,
            raw_data_preview: data.result.substring(0, 200)
          })
        };
      }
    }
    
    // Validate the parsed data
    const isValidPageview = parsed && parsed.timestamp && parsed.ip_address;
    
    if (isValidPageview) {
      // Transform to pageview format
      const pageview = {
        timestamp: parsed.timestamp,
        ip_address: parsed.ip_address,
        landing_page: parsed.landing_page || 'unknown',
        source: parsed.source || 'unknown',
        utm_campaign: parsed.utm_campaign,
        utm_medium: parsed.utm_medium,
        utm_source: parsed.utm_source,
        session_id: parsed.session_id,
        canvas_fingerprint: parsed.canvas_fingerprint,
        redis_key: testKey
      };
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'Successfully extracted pageview data!',
          parse_method: parseMethod,
          extracted_pageview: pageview,
          original_fields_count: Object.keys(parsed).length,
          fix_needed: 'Apply the parsing fix to extract-pageviews-chunked.js'
        })
      };
    } else {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'Data parsed but missing required fields',
          parse_method: parseMethod,
          has_timestamp: !!parsed?.timestamp,
          has_ip_address: !!parsed?.ip_address,
          available_fields: Object.keys(parsed || {}),
          parsed_data_sample: parsed
        })
      };
    }
    
  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Test failed', 
        message: error.message 
      })
    };
  }
};
