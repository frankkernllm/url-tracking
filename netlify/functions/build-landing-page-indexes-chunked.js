// netlify/functions/build-landing-page-indexes-chunked.js

const buildLandingPageIndexesChunked = async () => {
  console.log('Starting chunked landing page indexing with resume capability');
  
  // Redis client setup using fetch
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
      if (keys.length === 0) return [];
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
    },
    
    async set(key, value) {
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/set/${encodeURIComponent(key)}/${encodeURIComponent(value)}`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      });
      const data = await response.json();
      return data.result;
    }
  };

  const startTime = Date.now();
  const timeLimit = 20000; // 20-second safety margin
  
  // Get or initialize progress
  const progressKey = 'landing_page_chunked_progress';
  let progress = await redis.get(progressKey);
  if (progress) {
    progress = JSON.parse(progress);
    console.log(`Resuming from: ${progress.last_processed_hour}`);
  } else {
    progress = {
      start_date: null,
      end_date: null,
      hours_processed: 0,
      total_hours: 0,
      last_processed_hour: null,
      created_at: new Date().toISOString()
    };
  }

  // Get date range from query parameters or use defaults
  const getDateRange = (event) => {
    const params = event?.queryStringParameters || {};
    const startDate = params.start_date || '2025-07-01'; // Default to July 1st
    const endDate = params.end_date || new Date().toISOString().substring(0, 10); // Default to today
    const hoursPerChunk = parseInt(params.hours_per_chunk) || 6; // Process 6 hours at a time
    
    return { startDate, endDate, hoursPerChunk };
  };

  const generateHoursList = (startDate, endDate) => {
    const hours = [];
    const start = new Date(startDate + 'T00:00:00.000Z');
    const end = new Date(endDate + 'T23:59:59.999Z');
    
    for (let current = new Date(start); current <= end; current.setHours(current.getHours() + 1)) {
      const hourStr = current.toISOString().substring(0, 13); // 2025-07-19T14
      hours.push(hourStr);
    }
    
    return hours;
  };

  const processHourChunk = async (hours) => {
    const chunkResults = [];
    
    for (const hour of hours) {
      // Check timeout
      if (Date.now() - startTime > timeLimit) {
        console.log(`Timeout protection triggered during hour ${hour}`);
        return { timeout: true, processed: chunkResults };
      }

      const hourKey = `landing_page_hourly:${hour.replace('T', '-')}`;
      
      // Check if already processed
      const existing = await redis.get(hourKey);
      if (existing) {
        console.log(`Hour ${hour} already processed - skipping`);
        chunkResults.push({ hour, status: 'already_processed' });
        continue;
      }

      // Process this hour
      const hourResult = await processHour(hour, redis);
      chunkResults.push(hourResult);
      
      console.log(`Processed hour ${hour}: ${hourResult.summary}`);
    }
    
    return { timeout: false, processed: chunkResults };
  };

  const processHour = async (hour, redis) => {
    const hourStart = new Date(hour + ':00:00.000Z').getTime();
    const hourEnd = hourStart + (60 * 60 * 1000);
    const landingPageData = {};
    const ipTracker = new Set();
    
    // Get all IP indexes
    const ipIndexKeys = await redis.keys('pageview_index_ip:*');
    let processedCount = 0;
    let matchedPageviews = 0;
    
    // Process in smaller chunks to avoid timeout
    for (let i = 0; i < ipIndexKeys.length; i += 50) { // Smaller chunks for safety
      const chunk = ipIndexKeys.slice(i, i + 50);
      const chunkData = await redis.mget(chunk);
      
      chunkData.forEach(data => {
        if (!data) return;
        processedCount++;
        
        try {
          const pageviewData = JSON.parse(data);
          const timestamp = new Date(pageviewData.timestamp).getTime();
          
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
            
            if (!landingPageData[landingPage].unique_ips.includes(ip)) {
              landingPageData[landingPage].unique_ips.push(ip);
              landingPageData[landingPage].count++;
              ipTracker.add(ip);
              
              landingPageData[landingPage].sources[source] = 
                (landingPageData[landingPage].sources[source] || 0) + 1;
            }
          }
        } catch (parseError) {
          console.error(`Error parsing pageview data:`, parseError);
        }
      });
    }
    
    // Calculate percentages and clean up data
    const totalUniqueIPs = ipTracker.size;
    Object.entries(landingPageData).forEach(([page, pageData]) => {
      pageData.percentage = totalUniqueIPs > 0 ? 
        ((pageData.count / totalUniqueIPs) * 100).toFixed(1) : '0.0';
      delete pageData.unique_ips; // Remove IP arrays to save space
    });
    
    const finalIndex = {
      hour: hour,
      total_unique_ips: totalUniqueIPs,
      landing_pages: landingPageData,
      processing_stats: {
        pageview_records_scanned: processedCount,
        pageviews_in_hour: matchedPageviews,
        unique_ips_found: totalUniqueIPs,
        landing_pages_found: Object.keys(landingPageData).length
      },
      created_at: new Date().toISOString()
    };
    
    // Store with 90-day TTL
    const hourKey = `landing_page_hourly:${hour.replace('T', '-')}`;
    const ttl = 90 * 24 * 60 * 60;
    await redis.setex(hourKey, ttl, JSON.stringify(finalIndex));
    
    return {
      hour,
      status: 'processed',
      unique_ips: totalUniqueIPs,
      landing_pages: Object.keys(landingPageData).length,
      summary: `${totalUniqueIPs} unique visitors across ${Object.keys(landingPageData).length} landing pages`
    };
  };

  return { redis, processHourChunk, generateHoursList, getDateRange, progress, progressKey, startTime, timeLimit };
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

  console.log(`Chunked landing page indexing triggered at ${new Date().toISOString()}`);
  
  try {
    const { 
      redis, 
      processHourChunk, 
      generateHoursList, 
      getDateRange, 
      progress, 
      progressKey, 
      startTime, 
      timeLimit 
    } = await buildLandingPageIndexesChunked();

    const { startDate, endDate, hoursPerChunk } = getDateRange(event);
    
    // Generate complete list of hours to process
    const allHours = generateHoursList(startDate, endDate);
    
    // Update progress if this is a new date range
    if (!progress.start_date || progress.start_date !== startDate || progress.end_date !== endDate) {
      progress.start_date = startDate;
      progress.end_date = endDate;
      progress.total_hours = allHours.length;
      progress.hours_processed = 0;
      progress.last_processed_hour = null;
    }
    
    // Find where to resume
    let startIndex = 0;
    if (progress.last_processed_hour) {
      startIndex = allHours.findIndex(h => h > progress.last_processed_hour);
      if (startIndex === -1) startIndex = allHours.length; // All done
    }
    
    console.log(`Processing ${allHours.length} total hours from ${startDate} to ${endDate}`);
    console.log(`Starting from index ${startIndex} (${progress.hours_processed} already processed)`);
    
    const results = [];
    let currentIndex = startIndex;
    
    // Process in chunks with timeout protection
    while (currentIndex < allHours.length) {
      // Check timeout
      if (Date.now() - startTime > timeLimit) {
        console.log(`Timeout protection triggered - saving progress and exiting`);
        break;
      }
      
      // Get next chunk of hours
      const chunk = allHours.slice(currentIndex, currentIndex + hoursPerChunk);
      console.log(`Processing chunk: ${chunk[0]} to ${chunk[chunk.length - 1]} (${chunk.length} hours)`);
      
      const chunkResult = await processHourChunk(chunk);
      results.push(...chunkResult.processed);
      
      // Update progress
      currentIndex += chunk.length;
      progress.hours_processed += chunk.length;
      progress.last_processed_hour = chunk[chunk.length - 1];
      
      // Save progress
      await redis.setex(progressKey, 3600, JSON.stringify(progress)); // 1-hour TTL
      
      if (chunkResult.timeout) {
        console.log(`Chunk processing timed out - saving progress`);
        break;
      }
    }
    
    const isComplete = currentIndex >= allHours.length;
    const summary = {
      status: isComplete ? 'complete' : 'partial',
      date_range: { start: startDate, end: endDate },
      total_hours: allHours.length,
      hours_processed: progress.hours_processed,
      percentage_complete: ((progress.hours_processed / allHours.length) * 100).toFixed(1),
      execution_time_ms: Date.now() - startTime,
      results_summary: {
        hours_in_this_run: results.length,
        successful_hours: results.filter(r => r.status === 'processed').length,
        skipped_hours: results.filter(r => r.status === 'already_processed').length
      }
    };
    
    if (isComplete) {
      // Clear progress when complete
      await redis.set(progressKey, JSON.stringify({}));
      console.log(`Chunked indexing completed! Processed ${progress.hours_processed} hours total`);
    } else {
      console.log(`Chunked indexing partial completion: ${progress.hours_processed}/${allHours.length} hours (${summary.percentage_complete}%)`);
    }
    
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ...summary,
        next_action: isComplete ? 'Complete - run again for new date range' : 'Run again to continue processing'
      })
    };
    
  } catch (error) {
    console.error(`Chunked indexing failed:`, error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'Chunked indexing failed',
        message: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};
