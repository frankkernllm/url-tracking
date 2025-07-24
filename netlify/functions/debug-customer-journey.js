// .netlify/functions/debug-customer-journey.js
const createCorsResponse = (statusCode, body) => ({
  statusCode,
  headers: {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Content-Type': 'application/json'
  },
  body: JSON.stringify(body)
});

// Your Redis connection helper (copy from existing functions)
const withTimeout = (promise, timeoutMs = 25000) => {
  return Promise.race([
    promise,
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error(`Operation timed out after ${timeoutMs}ms`)), timeoutMs)
    )
  ]);
};

const redis = {
  async get(key) {
    const url = `https://us1-selected-sculpin-40778.upstash.io/get/${encodeURIComponent(key)}`;
    return withTimeout(
      fetch(url, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      }).then(r => r.json()).then(data => data.result),
      10000
    );
  },
  
  async hgetall(key) {
    const url = `https://us1-selected-sculpin-40778.upstash.io/hgetall/${encodeURIComponent(key)}`;
    return withTimeout(
      fetch(url, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      }).then(r => r.json()).then(data => {
        const result = data.result || [];
        const obj = {};
        for (let i = 0; i < result.length; i += 2) {
          obj[result[i]] = result[i + 1];
        }
        return obj;
      }),
      10000
    );
  },
  
  async keys(pattern) {
    const url = `https://us1-selected-sculpin-40778.upstash.io/keys/${encodeURIComponent(pattern)}`;
    return withTimeout(
      fetch(url, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      }).then(r => r.json()).then(data => data.result || []),
      15000
    );
  },
  
  async exists(key) {
    const url = `https://us1-selected-sculpin-40778.upstash.io/exists/${encodeURIComponent(key)}`;
    return withTimeout(
      fetch(url, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      }).then(r => r.json()).then(data => data.result === 1),
      5000
    );
  }
};

const DEBUG_CUSTOMER = {
  email: "bobbyfrostonline@gmail.com",
  conversion_timestamp: "2025-07-23T21:36:12.307Z",
  conversion_order_id: 1834813,
  expected_ips: ["2a09:bac3:a1c0:428::6a:8e", "104.28.39.73"],
  expected_session: "1753306347750-vvbs7grad"
};

async function debugCustomerJourneyLinkage() {
  const results = {
    pageviews_found: 0,
    conversions_found: 0,
    journeys_found: 0,
    indexes_exist: {},
    diagnosis: "",
    recommendations: []
  };
  
  console.log("🔍 DEBUGGING CUSTOMER JOURNEY LINKAGE");
  console.log("=====================================");
  
  try {
    // Step 1: Check raw pageview storage
    console.log("\n1️⃣ CHECKING RAW PAGEVIEW STORAGE");
    const pageviewKeys = await redis.keys("pageviews:*");
    console.log(`Found ${pageviewKeys.length} total pageview keys`);
    
    let customerPageviews = [];
    // Check recent pageviews (limit to avoid timeout)
    for (let i = Math.max(0, pageviewKeys.length - 500); i < pageviewKeys.length; i++) {
      try {
        const key = pageviewKeys[i];
        const pageview = await redis.hgetall(key);
        if (pageview.session_id === DEBUG_CUSTOMER.expected_session ||
            DEBUG_CUSTOMER.expected_ips.includes(pageview.ip_address)) {
          customerPageviews.push({key, ...pageview});
        }
      } catch (e) {
        // Skip invalid keys
      }
    }
    
    results.pageviews_found = customerPageviews.length;
    console.log(`✅ Found ${customerPageviews.length} pageviews for customer`);
    
    // Step 2: Check pageview indexes
    console.log("\n2️⃣ CHECKING PAGEVIEW INDEXES");
    for (let ip of DEBUG_CUSTOMER.expected_ips) {
      const encodedIP = ip.replace(/[:.]/g, '_');
      const indexKey = `pageview_index_ip:${encodedIP}`;
      
      const indexExists = await redis.exists(indexKey);
      results.indexes_exist[ip] = indexExists;
      console.log(`Index for IP ${ip}: ${indexExists ? '✅ EXISTS' : '❌ MISSING'}`);
    }
    
    // Step 3: Check conversion storage
    console.log("\n3️⃣ CHECKING CONVERSION STORAGE");
    const conversionKeys = await redis.keys("conversions:*");
    let customerConversions = [];
    
    for (let i = Math.max(0, conversionKeys.length - 200); i < conversionKeys.length; i++) {
      try {
        const key = conversionKeys[i];
        const conversion = await redis.hgetall(key);
        if (conversion.order_id == DEBUG_CUSTOMER.conversion_order_id ||
            conversion.customer_email === DEBUG_CUSTOMER.email) {
          customerConversions.push({key, ...conversion});
        }
      } catch (e) {
        // Skip invalid keys
      }
    }
    
    results.conversions_found = customerConversions.length;
    console.log(`✅ Found ${customerConversions.length} conversions for customer`);
    
    // Step 4: Check customer journey records
    console.log("\n4️⃣ CHECKING CUSTOMER JOURNEY RECORDS");
    const journeyKeys = await redis.keys("customer_journey:*");
    let customerJourneys = [];
    
    for (let key of journeyKeys) {
      try {
        const journey = await redis.hgetall(key);
        if (journey.conversion_order_id == DEBUG_CUSTOMER.conversion_order_id ||
            journey.customer_email === DEBUG_CUSTOMER.email) {
          customerJourneys.push({key, ...journey});
        }
      } catch (e) {
        // Skip invalid keys
      }
    }
    
    results.journeys_found = customerJourneys.length;
    console.log(`✅ Found ${customerJourneys.length} journey records`);
    
    // Step 5: Diagnosis
    console.log("\n5️⃣ DIAGNOSIS");
    
    const hasEmptyJourneys = customerJourneys.some(j => (j.total_touchpoints || 0) == 0);
    const allIndexesMissing = Object.values(results.indexes_exist).every(exists => !exists);
    
    if (customerPageviews.length > 0 && customerJourneys.length > 0 && hasEmptyJourneys) {
      results.diagnosis = "LINKAGE_ISSUE";
      results.recommendations = [
        "Re-run build-indexes-complete.js to rebuild pageview indexes",
        "Check IP encoding consistency between scripts",
        "Verify attribution window is >= 7 days in build-customer-journeys.js"
      ];
      console.log("❌ ISSUE: Pageviews exist but aren't linked to journeys");
    } else if (allIndexesMissing) {
      results.diagnosis = "MISSING_INDEXES";
      results.recommendations = [
        "Run extract-pageviews-chunked.js first",
        "Then run build-indexes-complete.js",
        "Finally run build-customer-journeys.js"
      ];
      console.log("❌ ISSUE: Pageview indexes are missing");
    } else if (customerPageviews.length === 0) {
      results.diagnosis = "NO_PAGEVIEWS";
      results.recommendations = [
        "Check if store-attribution.js is working",
        "Run extract-pageviews-chunked.js",
        "Verify pageview storage keys"
      ];
      console.log("❌ ISSUE: No pageviews found");
    } else {
      results.diagnosis = "WORKING";
      console.log("✅ Journey linkage appears to be working");
    }
    
  } catch (error) {
    console.error("Debug error:", error);
    results.error = error.message;
  }
  
  return results;
}

exports.handler = async (event, context) => {
  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return createCorsResponse(200, {});
  }
  
  // API key validation
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== 'ojoy_track_2025_secure_key_v1') {
    return createCorsResponse(401, { error: 'Invalid API key' });
  }
  
  try {
    const debugResults = await debugCustomerJourneyLinkage();
    
    return createCorsResponse(200, {
      debug_results: debugResults,
      customer: DEBUG_CUSTOMER,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('Debug function error:', error);
    return createCorsResponse(500, { 
      error: 'Debug failed',
      details: error.message 
    });
  }
};
