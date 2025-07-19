Let me rewrite this using the same Redis approach as your existing functions, without any external dependencies:

```javascript
// netlify/functions/build-landing-page-indexes-hourly.js

const buildHourlyLandingPageIndexes = async () => {
  const currentHour = new Date().toISOString().substring(0, 13); // 2025-07-19T14
  const hourKey = `landing_page_hourly:${currentHour.replace('T', '-')}`;
  
  console.log(`🕐 Starting hourly landing page indexing for ${currentHour}`);
  
  // Redis client setup using fetch (same as your other functions)
  const redis = {
    async get(key) {
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/get/${encodeURIComponent(key)}`, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      });
      const data = await response.json();
      return data.result;
    },
    
    async keys(pattern) {
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/keys/${encodeURIComponent(pattern)}`, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      });
      const data = await response.json();
      return data.result || [];
    },
    
    async mget(keys) {
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/mget/${keys.map(k => encodeURIComponent(k)).join('/')}`, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      });
      const data = await response.json();
      return data.result || [];
    },
    
    async setex(key, ttl, value) {
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/setex/${encodeURIComponent(key)}/${ttl}/${encodeURIComponent(value)}`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      });
      const data = await response.json();
      return data.result;
    }
  };
  
  // Check if already processed
  const existing = await redis.get(hourKey);
  if (existing) {
    console.log(`✅ Hour ${currentHour} already processed`);
    return { status: 'already_processed', hour: currentHour };
  }
  
  const startTime = Date.now();
  const landingPageData = {};
  const ipTracker = new Set();
  
  // Calculate hour boundaries
  const hourStart = new Date(currentHour + ':00:00.000Z').getTime();
  const hourEnd = hourStart + (60 * 60 * 1000); // +1 hour
  
  console.log(`📅 Processing pageviews from ${new Date(hourStart).toISOString()} to ${new Date(hourEnd).toISOString()}`);
  
  try {
    // Get all IP indexes
    const ipIndexKeys = await redis.keys('pageview_index_ip:*');
    console.log(`🔍 Found ${ipIndexKeys.length} unique IP indexes to scan`);
    
    let processedCount = 0;
    let matchedPageviews = 0;
    
    // Process in timeout-safe chunks
    for (let i = 0; i < ipIndexKeys.length; i += 100) {
      // Timeout protection
      if (Date.now() - startTime > 20000) {
        console.log(`⏰ Timeout protection triggered after ${Date.now() - startTime}ms`);
        break;
      }
      
      const chunk = ipIndexKeys.slice(i, i + 100);
      const chunkData = await redis.mget(chunk);
      
      chunkData.forEach(data => {
        if (!data) return;
        processedCount++;
        
        try {
          const pageviewData = JSON.parse(data);
          const timestamp = new Date(pageviewData.timestamp).getTime();
          
          // Only process pageviews from this specific hour
          if (timestamp >= hourStart && timestamp < hourEnd) {
            matchedPageviews++;
            const landingPage = pageviewData.landing_page || 'unknown';
            const ip = pageviewData.ip_address;
            const source = pageviewData.source || 'unknown';
            
            if (!landingPageData[landingPage]) {
              landingPageData[landingPage] = {
                unique_ips: [],
                count: 0,
                sources: {}
              };
            }
            
            // Deduplicate by IP for this landing page
            if (!landingPageData[landingPage].unique_ips.includes(ip)) {
              landingPageData[landingPage].unique_ips.push(ip);
              landingPageData[landingPage].count++;
              ipTracker.add(ip);
              
              // Track traffic sources
              landingPageData[landingPage].sources[source] = 
                (landingPageData[landingPage].sources[source] || 0) + 1;
            }
          }
        } catch (parseError) {
          console.error(`❌ Error parsing pageview data:`, parseError);
        }
      });
    }
    
    // Calculate percentages and clean up data
    const totalUniqueIPs = ipTracker.size;
    Object.entries(landingPageData).forEach(([page, pageData]) => {
      pageData.percentage = totalUniqueIPs > 0 ? 
        ((pageData.count / totalUniqueIPs) * 100).toFixed(1) : '0.0';
      
      // Remove the IP arrays to save space (keep only counts)
      delete pageData.unique_ips;
    });
    
    const finalIndex = {
      hour: currentHour,
      total_unique_ips: totalUniqueIPs,
      landing_pages: landingPageData,
      processing_stats: {
        processing_time_ms: Date.now() - startTime,
        pageview_records_scanned: processedCount,
        pageviews_in_hour: matchedPageviews,
        unique_ips_found: totalUniqueIPs,
        landing_pages_found: Object.keys(landingPageData).length
      },
      created_at: new Date().toISOString()
    };
    
    // Store with 90-day TTL (hourly retention)
    const ttl = 90 * 24 * 60 * 60; // 90 days in seconds
    await redis.setex(hourKey, ttl, JSON.stringify(finalIndex));
    
    console.log(`✅ Successfully indexed ${totalUniqueIPs} unique visitors across ${Object.keys(landingPageData).length} landing pages`);
    
    return { 
      status: 'processed', 
      data: finalIndex,
      summary: `Processed ${totalUniqueIPs} unique visitors across ${Object.keys(landingPageData).length} landing pages in ${Date.now() - startTime}ms`
    };
    
  } catch (error) {
    console.error(`❌ Error during hourly indexing:`, error);
    throw error;
  }
};

exports.handler = async (event, context) => {
  // API Key authentication
  const apiKey = event.headers['x-api-key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  console.log(`🚀 Manual landing page indexing triggered at ${new Date().toISOString()}`);
  
  try {
    const result = await buildHourlyLandingPageIndexes();
    console.log(`✅ Manual indexing completed:`, result.summary || result.status);
    
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(result)
    };
  } catch (error) {
    console.error(`❌ Manual indexing failed:`, error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'Indexing failed',
        message: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};
```

This version uses the same Redis approach as your existing functions (direct fetch calls to Upstash REST API) - no external dependencies needed!
