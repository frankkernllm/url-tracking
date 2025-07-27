// Attribution Index Builder - Multi-Index Creation for Fast Attribution Queries
// Path: netlify/functions/build-attribution-indexes.js
// Purpose: Build optimized indexes from attribution data chunks for fast attribution analysis

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
    console.log('🚀 ATTRIBUTION INDEX BUILDER - Multi-index creation starting...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Step 1: Find all attribution data chunks
    const allChunks = await findAllAttributionChunks(redis);
    console.log(`📦 Found ${allChunks.length} attribution data chunks`);
    
    if (allChunks.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No attribution data chunks found. Run extract-attribution-data.js first.'
        })
      };
    }
    
    // Step 2: Load existing progress or start fresh
    const progressKey = 'attribution_index_building_v1_progress';
    const progress = await getAttributionIndexProgress(redis, progressKey);
    
    // Auto-detect chunk changes and reset if needed
    const chunkMismatch = allChunks.length !== progress.total_chunks;
    if (chunkMismatch) {
      console.log(`🔧 Detected new chunks: ${progress.total_chunks} -> ${allChunks.length}, updating progress`);
      progress.total_chunks = allChunks.length;
    }
    
    // Check if already complete
    if (progress.is_complete && progress.last_chunk_index >= allChunks.length) {
      console.log('✅ Attribution index building already complete!');
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          attribution_indexing_summary: {
            build_complete: true,
            chunks_processed: progress.chunks_processed,
            total_chunks: progress.total_chunks,
            indexes_created: {
              ip_indexes: progress.ip_indexes_created,
              session_indexes: progress.session_indexes_created,
              landing_page_indexes: progress.landing_page_indexes_created,
              source_indexes: progress.source_indexes_created
            },
            completion_percentage: 100,
            completed_at: progress.completed_at
          },
          next_steps: [
            '✅ Attribution index building complete!',
            'All attribution indexes created successfully',
            'System ready for attribution queries',
            'Run attribution-query.js to perform attribution analysis'
          ]
        })
      };
    }
    
    // Step 3: Process remaining chunks
    const remainingChunks = allChunks.slice(progress.last_chunk_index);
    console.log(`📊 Attribution indexing plan: ${remainingChunks.length} chunks remaining`);
    
    const indexingResult = await processChunksAndBuildAttributionIndexes(
      redis, 
      remainingChunks, 
      progress,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Step 4: Update progress
    progress.chunks_processed += indexingResult.chunks_processed_this_run;
    progress.last_chunk_index += indexingResult.chunks_processed_this_run;
    progress.ip_indexes_created += indexingResult.ip_indexes_created_this_run;
    progress.session_indexes_created += indexingResult.session_indexes_created_this_run;
    progress.landing_page_indexes_created += indexingResult.landing_page_indexes_created_this_run;
    progress.source_indexes_created += indexingResult.source_indexes_created_this_run;
    
    // Check if complete
    if (progress.last_chunk_index >= allChunks.length) {
      progress.is_complete = true;
      progress.completed_at = new Date().toISOString();
      console.log('🎉 Attribution index building completed!');
    }
    
    await saveAttributionIndexProgress(redis, progressKey, progress);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Attribution indexing finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        attribution_indexing_summary: {
          // This run stats
          chunks_processed_this_run: indexingResult.chunks_processed_this_run,
          processing_time_ms: totalTime,
          
          // Index creation stats for this run
          indexes_created_this_run: {
            ip_indexes: indexingResult.ip_indexes_created_this_run,
            session_indexes: indexingResult.session_indexes_created_this_run,
            landing_page_indexes: indexingResult.landing_page_indexes_created_this_run,
            source_indexes: indexingResult.source_indexes_created_this_run
          },
          
          // Total progress
          build_complete: progress.is_complete,
          progress: {
            chunks_processed: progress.chunks_processed,
            total_chunks: progress.total_chunks,
            completion_percentage: ((progress.chunks_processed / progress.total_chunks) * 100).toFixed(1)
          },
          
          // Total indexes created
          total_indexes_created: {
            ip_indexes: progress.ip_indexes_created,
            session_indexes: progress.session_indexes_created,
            landing_page_indexes: progress.landing_page_indexes_created,
            source_indexes: progress.source_indexes_created,
            total: progress.ip_indexes_created + progress.session_indexes_created + 
                   progress.landing_page_indexes_created + progress.source_indexes_created
          },
          
          // Attribution capabilities
          attribution_readiness: {
            multi_index_attribution: true,
            session_based_matching: progress.session_indexes_created > 0,
            ip_based_matching: progress.ip_indexes_created > 0,
            landing_page_analysis: progress.landing_page_indexes_created > 0,
            source_analysis: progress.source_indexes_created > 0,
            time_range_covered: indexingResult.time_range
          }
        },
        
        next_steps: progress.is_complete ? [
          '🎉 Attribution index building complete!',
          'All multi-index attribution indexes created',
          'System ready for first-touch/last-touch attribution analysis',
          'Run attribution-query.js to perform attribution queries'
        ] : [
          'Attribution index building in progress...',
          'Run the same command again to continue',
          `Progress: ${progress.chunks_processed}/${progress.total_chunks} chunks`,
          `Estimated chunks remaining: ${progress.total_chunks - progress.chunks_processed}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Attribution indexing failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Attribution indexing failed', 
        message: error.message 
      })
    };
  }
};

// Get attribution index building progress
async function getAttributionIndexProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing attribution index progress: chunk ${progress.last_chunk_index}/${progress.total_chunks}`);
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing attribution index progress found, starting fresh');
  }
  
  return {
    last_chunk_index: 0,
    total_chunks: 0,
    ip_indexes_created: 0,
    session_indexes_created: 0,
    landing_page_indexes_created: 0,
    source_indexes_created: 0,
    chunks_processed: 0,
    started_at: new Date().toISOString(),
    is_complete: false
  };
}

// Save attribution index progress
async function saveAttributionIndexProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/7200/${encodeURIComponent(JSON.stringify(progress))}`); // 2 hour TTL
  console.log(`💾 Attribution index progress saved: chunk ${progress.last_chunk_index}/${progress.total_chunks}`);
}

// Find all attribution data chunks
async function findAllAttributionChunks(redis) {
  console.log('🔍 Finding all attribution data chunks...');
  let cursor = '0';
  let allChunks = [];
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/attribution_data_chunk:v1_*/count/1000`);
      
      if (scanResult?.result && Array.isArray(scanResult.result) && scanResult.result.length >= 2) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        allChunks.push(...keys);
        
        console.log(`📦 Found ${keys.length} attribution chunks, cursor: ${cursor}, total: ${allChunks.length}`);
      } else {
        cursor = '0';
      }
    } catch (error) {
      console.log(`⚠️ Error scanning for attribution chunks: ${error.message}`);
      break;
    }
  } while (cursor !== '0');
  
  console.log(`✅ Found ${allChunks.length} total attribution data chunks`);
  return allChunks;
}

// Process chunks and build attribution indexes
async function processChunksAndBuildAttributionIndexes(redis, chunkKeys, progress, maxTime) {
  const processStartTime = Date.now();
  console.log(`⚡ Processing ${chunkKeys.length} attribution chunks to build multi-indexes in ${maxTime}ms`);
  
  // Index maps for efficient processing
  const ipIndexMap = new Map();
  const sessionIndexMap = new Map();
  const landingPageIndexMap = new Map();
  const sourceIndexMap = new Map();
  
  const timeStats = { earliest: null, latest: null };
  let totalPageviews = 0;
  let chunksProcessedThisRun = 0;
  let attributionFieldsFound = new Set();
  
  // Step 1: Process chunks and collect data for indexes
  for (let i = 0; i < chunkKeys.length; i++) {
    // Time check
    if (Date.now() - processStartTime > maxTime - 5000) {
      console.log(`⏰ Time limit approaching, stopping at chunk ${i}/${chunkKeys.length}`);
      break;
    }
    
    try {
      const chunkKey = chunkKeys[i];
      console.log(`📦 Processing attribution chunk ${progress.last_chunk_index + i + 1}/${progress.total_chunks}: ${chunkKey}`);
      
      const chunkData = await redis(`get/${chunkKey}`, 2000);
      
      if (chunkData?.result) {
        const chunk = JSON.parse(decodeURIComponent(chunkData.result));
        
        if (chunk.pageviews && Array.isArray(chunk.pageviews)) {
          for (const pageview of chunk.pageviews) {
            totalPageviews++;
            
            // Track time range
            const pvTime = new Date(pageview.timestamp);
            if (!timeStats.earliest || pvTime < timeStats.earliest) {
              timeStats.earliest = pvTime;
            }
            if (!timeStats.latest || pvTime > timeStats.latest) {
              timeStats.latest = pvTime;
            }
            
            // Track attribution fields
            Object.keys(pageview).forEach(field => attributionFieldsFound.add(field));
            
            // Build IP index data
            if (pageview.ip_address && pageview.ip_address !== 'unknown') {
              const encodedIP = encodeIPForKey(pageview.ip_address);
              
              if (!ipIndexMap.has(encodedIP)) {
                ipIndexMap.set(encodedIP, {
                  ip_address: pageview.ip_address,
                  pageviews: [],
                  session_ids: new Set(),
                  landing_pages: new Set(),
                  sources: new Set(),
                  latest_timestamp: pageview.timestamp,
                  earliest_timestamp: pageview.timestamp
                });
              }
              
              const ipData = ipIndexMap.get(encodedIP);
              ipData.pageviews.push(pageview);
              
              // Track associated data
              if (pageview.session_id) ipData.session_ids.add(pageview.session_id);
              if (pageview.landing_page) ipData.landing_pages.add(pageview.landing_page);
              if (pageview.source) ipData.sources.add(pageview.source);
              
              // Update timestamps
              if (new Date(pageview.timestamp) > new Date(ipData.latest_timestamp)) {
                ipData.latest_timestamp = pageview.timestamp;
              }
              if (new Date(pageview.timestamp) < new Date(ipData.earliest_timestamp)) {
                ipData.earliest_timestamp = pageview.timestamp;
              }
            }
            
            // Build Session index data
            if (pageview.session_id) {
              if (!sessionIndexMap.has(pageview.session_id)) {
                sessionIndexMap.set(pageview.session_id, {
                  session_id: pageview.session_id,
                  pageviews: [],
                  ip_addresses: new Set(),
                  landing_pages: new Set(),
                  sources: new Set(),
                  latest_timestamp: pageview.timestamp,
                  earliest_timestamp: pageview.timestamp
                });
              }
              
              const sessionData = sessionIndexMap.get(pageview.session_id);
              sessionData.pageviews.push(pageview);
              
              if (pageview.ip_address) sessionData.ip_addresses.add(pageview.ip_address);
              if (pageview.landing_page) sessionData.landing_pages.add(pageview.landing_page);
              if (pageview.source) sessionData.sources.add(pageview.source);
              
              if (new Date(pageview.timestamp) > new Date(sessionData.latest_timestamp)) {
                sessionData.latest_timestamp = pageview.timestamp;
              }
              if (new Date(pageview.timestamp) < new Date(sessionData.earliest_timestamp)) {
                sessionData.earliest_timestamp = pageview.timestamp;
              }
            }
            
            // Build Landing Page index data
            if (pageview.landing_page && pageview.landing_page !== 'unknown') {
              const encodedLP = encodeLandingPageForKey(pageview.landing_page);
              
              if (!landingPageIndexMap.has(encodedLP)) {
                landingPageIndexMap.set(encodedLP, {
                  landing_page: pageview.landing_page,
                  pageviews: [],
                  ip_addresses: new Set(),
                  session_ids: new Set(),
                  sources: new Set(),
                  latest_timestamp: pageview.timestamp
                });
              }
              
              const lpData = landingPageIndexMap.get(encodedLP);
              lpData.pageviews.push(pageview);
              
              if (pageview.ip_address) lpData.ip_addresses.add(pageview.ip_address);
              if (pageview.session_id) lpData.session_ids.add(pageview.session_id);
              if (pageview.source) lpData.sources.add(pageview.source);
              
              if (new Date(pageview.timestamp) > new Date(lpData.latest_timestamp)) {
                lpData.latest_timestamp = pageview.timestamp;
              }
            }
            
            // Build Source index data
            if (pageview.source && pageview.source !== 'direct') {
              const encodedSource = encodeSourceForKey(pageview.source);
              
              if (!sourceIndexMap.has(encodedSource)) {
                sourceIndexMap.set(encodedSource, {
                  source: pageview.source,
                  pageviews: [],
                  ip_addresses: new Set(),
                  session_ids: new Set(),
                  landing_pages: new Set(),
                  latest_timestamp: pageview.timestamp
                });
              }
              
              const sourceData = sourceIndexMap.get(encodedSource);
              sourceData.pageviews.push(pageview);
              
              if (pageview.ip_address) sourceData.ip_addresses.add(pageview.ip_address);
              if (pageview.session_id) sourceData.session_ids.add(pageview.session_id);
              if (pageview.landing_page) sourceData.landing_pages.add(pageview.landing_page);
              
              if (new Date(pageview.timestamp) > new Date(sourceData.latest_timestamp)) {
                sourceData.latest_timestamp = pageview.timestamp;
              }
            }
          }
        }
      }
      
      chunksProcessedThisRun++;
      
      // Save progress every 10 chunks
      if (chunksProcessedThisRun % 10 === 0) {
        const tempProgress = {
          ...progress,
          chunks_processed: progress.chunks_processed + chunksProcessedThisRun,
          last_chunk_index: progress.last_chunk_index + chunksProcessedThisRun
        };
        await saveAttributionIndexProgress(redis, 'attribution_index_building_v1_progress', tempProgress);
      }
      
    } catch (chunkError) {
      console.log(`⚠️ Error processing attribution chunk ${chunkKeys[i]}: ${chunkError.message}`);
    }
  }
  
  console.log(`📊 Attribution data collection complete:`);
  console.log(`   📦 Chunks processed: ${chunksProcessedThisRun}`);
  console.log(`   📝 Pageviews processed: ${totalPageviews}`);
  console.log(`   🌐 Unique IPs for indexing: ${ipIndexMap.size}`);
  console.log(`   🔗 Unique sessions for indexing: ${sessionIndexMap.size}`);
  console.log(`   📄 Unique landing pages for indexing: ${landingPageIndexMap.size}`);
  console.log(`   📊 Unique sources for indexing: ${sourceIndexMap.size}`);
  
  // Step 2: Build all index types
  const remainingTime = maxTime - (Date.now() - processStartTime);
  const indexCreationResults = await createAllAttributionIndexes(
    redis, 
    {
      ipIndexMap,
      sessionIndexMap,
      landingPageIndexMap,
      sourceIndexMap
    },
    remainingTime - 1000
  );
  
  const indexingTime = Date.now() - processStartTime;
  console.log(`✅ Attribution indexing completed in ${indexingTime}ms`);
  
  return {
    chunks_processed_this_run: chunksProcessedThisRun,
    pageviews_processed_this_run: totalPageviews,
    ip_indexes_created_this_run: indexCreationResults.ip_indexes_created,
    session_indexes_created_this_run: indexCreationResults.session_indexes_created,
    landing_page_indexes_created_this_run: indexCreationResults.landing_page_indexes_created,
    source_indexes_created_this_run: indexCreationResults.source_indexes_created,
    processing_time_ms: indexingTime,
    attribution_fields_found: Array.from(attributionFieldsFound),
    time_range: {
      earliest: timeStats.earliest?.toISOString(),
      latest: timeStats.latest?.toISOString(),
      span_days: timeStats.earliest && timeStats.latest 
        ? Math.ceil((timeStats.latest - timeStats.earliest) / (1000 * 60 * 60 * 24))
        : 0
    }
  };
}

// Create all attribution indexes
async function createAllAttributionIndexes(redis, indexMaps, maxTime) {
  const indexStartTime = Date.now();
  let ipIndexesCreated = 0;
  let sessionIndexesCreated = 0;
  let landingPageIndexesCreated = 0;
  let sourceIndexesCreated = 0;
  
  console.log(`🏗️ Creating attribution indexes with ${maxTime}ms available...`);
  
  try {
    // Create IP indexes
    console.log(`🌐 Creating ${indexMaps.ipIndexMap.size} IP indexes...`);
    ipIndexesCreated = await createIPIndexes(redis, indexMaps.ipIndexMap, Math.floor(maxTime * 0.4));
    
    // Create Session indexes
    if (Date.now() - indexStartTime < maxTime * 0.8) {
      console.log(`🔗 Creating ${indexMaps.sessionIndexMap.size} session indexes...`);
      sessionIndexesCreated = await createSessionIndexes(redis, indexMaps.sessionIndexMap, Math.floor(maxTime * 0.2));
    }
    
    // Create Landing Page indexes
    if (Date.now() - indexStartTime < maxTime * 0.9) {
      console.log(`📄 Creating ${indexMaps.landingPageIndexMap.size} landing page indexes...`);
      landingPageIndexesCreated = await createLandingPageIndexes(redis, indexMaps.landingPageIndexMap, Math.floor(maxTime * 0.2));
    }
    
    // Create Source indexes
    if (Date.now() - indexStartTime < maxTime * 0.95) {
      console.log(`📊 Creating ${indexMaps.sourceIndexMap.size} source indexes...`);
      sourceIndexesCreated = await createSourceIndexes(redis, indexMaps.sourceIndexMap, Math.floor(maxTime * 0.2));
    }
    
  } catch (error) {
    console.error('❌ Index creation error:', error);
  }
  
  const indexTime = Date.now() - indexStartTime;
  console.log(`✅ All attribution indexes created in ${indexTime}ms`);
  
  return {
    ip_indexes_created: ipIndexesCreated,
    session_indexes_created: sessionIndexesCreated,
    landing_page_indexes_created: landingPageIndexesCreated,
    source_indexes_created: sourceIndexesCreated
  };
}

// Create IP-based attribution indexes
async function createIPIndexes(redis, ipIndexMap, maxTime) {
  const startTime = Date.now();
  const ipEntries = Array.from(ipIndexMap.entries());
  const batchSize = 50;
  let created = 0;
  
  for (let i = 0; i < ipEntries.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = ipEntries.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([encodedIP, ipData]) => {
      try {
        // Sort pageviews by timestamp (most recent first)
        ipData.pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
        const ipKey = `attribution_index_v1_ip:${encodedIP}`;
        
        const indexData = {
          ip_address: ipData.ip_address,
          pageview_count: ipData.pageviews.length,
          latest_timestamp: ipData.latest_timestamp,
          earliest_timestamp: ipData.earliest_timestamp,
          pageviews: ipData.pageviews,
          
          // Attribution metadata
          session_ids: Array.from(ipData.session_ids),
          landing_pages: Array.from(ipData.landing_pages),
          sources: Array.from(ipData.sources),
          
          // Index metadata
          created_at: new Date().toISOString(),
          version: 'v1',
          attribution_ready: true,
          index_type: 'ip_attribution'
        };
        
        await redis(`setex/${ipKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 2000); // 30 days
        return 1;
        
      } catch (error) {
        console.log(`⚠️ Error creating IP index: ${error.message}`);
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} IP attribution indexes`);
  return created;
}

// Create Session-based attribution indexes
async function createSessionIndexes(redis, sessionIndexMap, maxTime) {
  const startTime = Date.now();
  const sessionEntries = Array.from(sessionIndexMap.entries());
  const batchSize = 50;
  let created = 0;
  
  for (let i = 0; i < sessionEntries.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = sessionEntries.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([sessionId, sessionData]) => {
      try {
        sessionData.pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
        const sessionKey = `attribution_index_v1_session:${sessionId}`;
        
        const indexData = {
          session_id: sessionData.session_id,
          pageview_count: sessionData.pageviews.length,
          latest_timestamp: sessionData.latest_timestamp,
          earliest_timestamp: sessionData.earliest_timestamp,
          pageviews: sessionData.pageviews,
          
          ip_addresses: Array.from(sessionData.ip_addresses),
          landing_pages: Array.from(sessionData.landing_pages),
          sources: Array.from(sessionData.sources),
          
          created_at: new Date().toISOString(),
          version: 'v1',
          attribution_ready: true,
          index_type: 'session_attribution'
        };
        
        await redis(`setex/${sessionKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 2000);
        return 1;
        
      } catch (error) {
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} session attribution indexes`);
  return created;
}

// Create Landing Page attribution indexes
async function createLandingPageIndexes(redis, landingPageIndexMap, maxTime) {
  const startTime = Date.now();
  const lpEntries = Array.from(landingPageIndexMap.entries());
  const batchSize = 50;
  let created = 0;
  
  for (let i = 0; i < lpEntries.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = lpEntries.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([encodedLP, lpData]) => {
      try {
        lpData.pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
        const lpKey = `attribution_index_v1_landing:${encodedLP}`;
        
        const indexData = {
          landing_page: lpData.landing_page,
          pageview_count: lpData.pageviews.length,
          latest_timestamp: lpData.latest_timestamp,
          pageviews: lpData.pageviews,
          
          ip_addresses: Array.from(lpData.ip_addresses),
          session_ids: Array.from(lpData.session_ids),
          sources: Array.from(lpData.sources),
          
          created_at: new Date().toISOString(),
          version: 'v1',
          attribution_ready: true,
          index_type: 'landing_page_attribution'
        };
        
        await redis(`setex/${lpKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 2000);
        return 1;
        
      } catch (error) {
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} landing page attribution indexes`);
  return created;
}

// Create Source attribution indexes
async function createSourceIndexes(redis, sourceIndexMap, maxTime) {
  const startTime = Date.now();
  const sourceEntries = Array.from(sourceIndexMap.entries());
  const batchSize = 50;
  let created = 0;
  
  for (let i = 0; i < sourceEntries.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = sourceEntries.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([encodedSource, sourceData]) => {
      try {
        sourceData.pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
        const sourceKey = `attribution_index_v1_source:${encodedSource}`;
        
        const indexData = {
          source: sourceData.source,
          pageview_count: sourceData.pageviews.length,
          latest_timestamp: sourceData.latest_timestamp,
          pageviews: sourceData.pageviews,
          
          ip_addresses: Array.from(sourceData.ip_addresses),
          session_ids: Array.from(sourceData.session_ids),
          landing_pages: Array.from(sourceData.landing_pages),
          
          created_at: new Date().toISOString(),
          version: 'v1',
          attribution_ready: true,
          index_type: 'source_attribution'
        };
        
        await redis(`setex/${sourceKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 2000);
        return 1;
        
      } catch (error) {
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} source attribution indexes`);
  return created;
}

// Utility functions for encoding keys
function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_').replace(/\./g, '_');
}

function encodeLandingPageForKey(landingPage) {
  return encodeURIComponent(landingPage).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 100);
}

function encodeSourceForKey(source) {
  return encodeURIComponent(source).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 50);
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
