// Conversion Data Extractor and Indexer v2
// Path: netlify/functions/extract-conversion-data-v2.js
// Purpose: Extract conversions stored by track.js and build fast lookup indexes (v2)

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
    const maxProcessingTime = 25000; // 25 seconds max
    
    console.log('🚀 Starting CONVERSION EXTRACTION and INDEXING...');
    
    // Load existing progress or start fresh (v2)
    const progressKey = 'conversion_extraction_v2_progress';
    const existingProgress = await getConversionProgress(redis, progressKey);
    
    console.log(`📊 Resuming conversion extraction:`, {
      total_extracted: existingProgress.total_extracted,
      total_indexed: existingProgress.total_indexed,
      last_cursor: existingProgress.last_cursor,
      chunks_completed: existingProgress.chunks_completed
    });
    
    // Extract and index conversions with smart resume
    const extractionResult = await extractAndIndexConversions(
      redis, 
      existingProgress,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Update progress after extraction
    const updatedProgress = {
      ...existingProgress,
      total_extracted: existingProgress.total_extracted + extractionResult.conversions_extracted_this_run,
      total_filtered_out: (existingProgress.total_filtered_out || 0) + (extractionResult.conversions_filtered_out_this_run || 0),
      total_indexed: existingProgress.total_indexed + extractionResult.indexes_created_this_run,
      total_keys_scanned: existingProgress.total_keys_scanned + extractionResult.keys_scanned_this_run,
      last_cursor: extractionResult.final_cursor,
      chunks_completed: existingProgress.chunks_completed + extractionResult.chunks_processed_this_run,
      unique_emails_found: extractionResult.total_unique_emails,
      unique_ips_found: extractionResult.total_unique_ips,
      unique_sessions_found: extractionResult.total_unique_sessions,
      last_updated: new Date().toISOString(),
      is_complete: extractionResult.is_complete,
      earliest_conversion: extractionResult.earliest_conversion || existingProgress.earliest_conversion,
      latest_conversion: extractionResult.latest_conversion || existingProgress.latest_conversion
    };
    
    await storeConversionProgress(redis, progressKey, updatedProgress);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ CONVERSION extraction and indexing finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_complete: extractionResult.is_complete,
        conversion_extraction_summary: {
          // This run stats
          conversions_extracted_this_run: extractionResult.conversions_extracted_this_run,
          conversions_filtered_out_this_run: extractionResult.conversions_filtered_out_this_run || 0,
          indexes_created_this_run: extractionResult.indexes_created_this_run,
          keys_scanned_this_run: extractionResult.keys_scanned_this_run,
          processing_time_ms: totalTime,
          
          // Total stats across all runs
          total_conversions_extracted: updatedProgress.total_extracted,
          total_conversions_filtered_out: updatedProgress.total_filtered_out || 0,
          total_indexes_created: updatedProgress.total_indexed,
          total_keys_scanned: updatedProgress.total_keys_scanned,
          extraction_method: 'conversion_extraction_indexing_v1',
          
          // Data quality stats
          extraction_rate: updatedProgress.total_keys_scanned > 0 
            ? `${((updatedProgress.total_extracted / updatedProgress.total_keys_scanned) * 100).toFixed(1)}%`
            : '0%',
          filter_rate: updatedProgress.total_keys_scanned > 0
            ? `${(((updatedProgress.total_filtered_out || 0) / updatedProgress.total_keys_scanned) * 100).toFixed(1)}%`
            : '0%'
        },
        conversion_coverage: {
          unique_emails_found: updatedProgress.unique_emails_found,
          unique_ips_found: updatedProgress.unique_ips_found,
          unique_sessions_found: updatedProgress.unique_sessions_found,
          earliest_conversion: updatedProgress.earliest_conversion,
          latest_conversion: updatedProgress.latest_conversion
        },
        index_types_created: [
          'conversion_index_v1_email',
          'conversion_index_v1_ip', 
          'conversion_index_v1_session',
          'conversion_index_v1_date'
        ],
        performance: {
          conversions_per_second_this_run: Math.round(extractionResult.conversions_extracted_this_run / (totalTime / 1000)),
          indexes_per_second_this_run: Math.round(extractionResult.indexes_created_this_run / (totalTime / 1000))
        },
        next_steps: extractionResult.is_complete ? [
          '✅ Conversion extraction and indexing complete!',
          'All conversion data from track.js has been processed',
          'Conversion indexes created for fast lookup',
          'System ready for conversion queries and analysis'
        ] : [
          'Conversion extraction continuing...',
          'Run the same command again to continue processing',
          'Progress is automatically saved and will resume from where it left off',
          `Next run will start from cursor: ${extractionResult.final_cursor}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Conversion extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Conversion extraction failed', 
        message: error.message 
      })
    };
  }
};

// Get conversion extraction progress (v2)
async function getConversionProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing conversion progress: ${progress.total_extracted} conversions extracted`);
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing conversion progress found, starting fresh');
  }
  
  // Default fresh start
  return {
    total_extracted: 0,
    total_indexed: 0,
    total_filtered_out: 0, // Track conversions filtered out for missing emails
    total_keys_scanned: 0,
    last_cursor: '0',
    chunks_completed: 0,
    unique_emails_found: 0,
    unique_ips_found: 0,
    unique_sessions_found: 0,
    started_at: new Date().toISOString(),
    is_complete: false,
    earliest_conversion: null,
    latest_conversion: null
  };
}

// Store conversion progress (v2)
async function storeConversionProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/3600/${encodeURIComponent(JSON.stringify(progress))}`); // 1 hour TTL
  console.log(`💾 Conversion progress saved: ${progress.total_extracted} total conversions, ${progress.total_indexed} indexes created`);
}

// Extract and index conversions with smart resume
async function extractAndIndexConversions(redis, existingProgress, maxTime) {
  const extractionStartTime = Date.now();
  let thisRunConversions = [];
  let thisRunKeysScanned = 0;
  let thisRunChunksProcessed = 0;
  let thisRunIndexesCreated = 0;
  let thisRunFilteredOut = 0; // Track conversions filtered out for missing emails
  let allUniqueEmails = new Set();
  let allUniqueIPs = new Set();
  let allUniqueSessions = new Set();
  let earliestConversion = existingProgress.earliest_conversion;
  let latestConversion = existingProgress.latest_conversion;
  
  let currentCursor = existingProgress.last_cursor;
  
  console.log(`🔄 CONVERSION EXTRACTION RESUME: Starting from cursor: ${currentCursor}`);
  console.log(`📊 Previous progress: ${existingProgress.total_extracted} conversions extracted`);
  
  try {
    // Extract conversions from track.js data
    do {
      // Check time remaining
      const timeRemaining = maxTime - (Date.now() - extractionStartTime);
      if (timeRemaining < 3000) {
        console.log(`⏰ Time limit approaching: ${timeRemaining}ms remaining, stopping extraction`);
        break;
      }
      
      console.log(`🔍 Processing conversion cursor: ${currentCursor}, this run: ${thisRunConversions.length} conversions`);
      
      // Scan for conversion keys from track.js
      const scanResult = await redis(`scan/${currentCursor}/match/conversions:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log(`🏁 Conversion scan complete: no more results`);
        currentCursor = '0'; // Mark as complete
        break;
      }
      
      currentCursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      thisRunKeysScanned += keys.length;
      thisRunChunksProcessed++;
      
      console.log(`📊 Conversion chunk ${thisRunChunksProcessed}: Found ${keys.length} keys, cursor: ${currentCursor}`);
      
      // Process conversion keys in batches
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const timeCheck = maxTime - (Date.now() - extractionStartTime);
        if (timeCheck < 2000) {
          console.log(`⏰ Time limit during batch processing`);
          break;
        }
        
        const batch = keys.slice(i, i + batchSize);
        const batchResults = await processConversionKeyBatch(redis, batch);
        
        // Track filtering stats
        const totalProcessed = batch.length;
        const validConversions = batchResults.length;
        const filteredOut = totalProcessed - validConversions;
        thisRunFilteredOut += filteredOut;
        
        // Extract conversion data and track unique values
        batchResults.forEach(conv => {
          if (conv && conv.timestamp && conv.email) {
            // Track unique values
            allUniqueEmails.add(conv.email);
            if (conv.conversion_ip) allUniqueIPs.add(conv.conversion_ip);
            if (conv.primary_ip) allUniqueIPs.add(conv.primary_ip);
            if (conv.unique_ips && Array.isArray(conv.unique_ips)) {
              conv.unique_ips.forEach(ip => allUniqueIPs.add(ip));
            }
            if (conv.ssid) allUniqueSessions.add(conv.ssid);
            
            // Track time range
            const convTime = new Date(conv.timestamp);
            if (!earliestConversion || convTime < new Date(earliestConversion)) {
              earliestConversion = conv.timestamp;
            }
            if (!latestConversion || convTime > new Date(latestConversion)) {
              latestConversion = conv.timestamp;
            }
            
            thisRunConversions.push(conv);
          }
        });
        
        console.log(`📦 Conversion batch processed: ${validConversions} valid conversions, ${filteredOut} filtered out (no email)`);
      }
      
      // Safety check: don't run forever
      if (thisRunChunksProcessed >= 50) {
        console.log(`🛑 Conversion safety limit: processed 50 chunks, stopping`);
        break;
      }
      
    } while (currentCursor !== '0' && Date.now() - extractionStartTime < maxTime - 5000);
    
    // Build conversion indexes from extracted data (v2)
    if (thisRunConversions.length > 0) {
      console.log(`🏗️ Building conversion indexes for ${thisRunConversions.length} conversions...`);
      const indexingResult = await buildConversionIndexes(redis, thisRunConversions, maxTime - (Date.now() - extractionStartTime) - 2000);
      thisRunIndexesCreated = indexingResult.indexes_created;
      console.log(`✅ Created ${thisRunIndexesCreated} conversion indexes`);
    }
    
    const isComplete = currentCursor === '0';
    const processingTime = Date.now() - extractionStartTime;
    
    console.log(`🏁 CONVERSION extraction summary:`);
    console.log(`   📊 This run conversions: ${thisRunConversions.length}`);
    console.log(`   📊 Total conversions: ${existingProgress.total_extracted + thisRunConversions.length}`);
    console.log(`   🔍 This run keys scanned: ${thisRunKeysScanned}`);
    console.log(`   ❌ This run filtered out (no email): ${thisRunFilteredOut}`);
    console.log(`   📦 This run chunks: ${thisRunChunksProcessed}`);
    console.log(`   🏗️ This run indexes created: ${thisRunIndexesCreated}`);
    console.log(`   📧 Unique emails found: ${allUniqueEmails.size}`);
    console.log(`   🌐 Unique IPs found: ${allUniqueIPs.size}`);
    console.log(`   🔗 Unique sessions found: ${allUniqueSessions.size}`);
    console.log(`   ✅ Complete: ${isComplete}`);
    console.log(`   ⏱️ This run time: ${processingTime}ms`);
    
    return {
      conversions_extracted_this_run: thisRunConversions.length,
      conversions_filtered_out_this_run: thisRunFilteredOut,
      indexes_created_this_run: thisRunIndexesCreated,
      keys_scanned_this_run: thisRunKeysScanned,
      chunks_processed_this_run: thisRunChunksProcessed,
      total_unique_emails: allUniqueEmails.size,
      total_unique_ips: allUniqueIPs.size,
      total_unique_sessions: allUniqueSessions.size,
      earliest_conversion: earliestConversion,
      latest_conversion: latestConversion,
      is_complete: isComplete,
      final_cursor: currentCursor,
      processing_time_ms: processingTime
    };
    
  } catch (error) {
    console.error('❌ Conversion extraction error:', error);
    return {
      conversions_extracted_this_run: thisRunConversions.length,
      conversions_filtered_out_this_run: thisRunFilteredOut,
      indexes_created_this_run: thisRunIndexesCreated,
      keys_scanned_this_run: thisRunKeysScanned,
      chunks_processed_this_run: thisRunChunksProcessed,
      total_unique_emails: allUniqueEmails.size,
      total_unique_ips: allUniqueIPs.size,
      total_unique_sessions: allUniqueSessions.size,
      is_complete: false,
      final_cursor: currentCursor,
      error: error.message
    };
  }
}

// Process batch of conversion keys
async function processConversionKeyBatch(redis, keys) {
  const batchPromises = keys.map(async (key) => {
    try {
      const result = await redis(`get/${key}`, 1000);
      if (result?.result) {
        let parsed;
        try {
          parsed = JSON.parse(result.result);
        } catch (parseError) {
          try {
            parsed = JSON.parse(decodeURIComponent(result.result));
          } catch (decodeError) {
            return null;
          }
        }
        
        if (parsed && parsed.timestamp) {
          // FILTER: Only process conversions with valid email addresses
          if (!parsed.email || parsed.email === 'unknown' || parsed.email === '' || parsed.email === null) {
            console.log(`⚠️ Skipping conversion with no email: ${key.substring(0, 50)}...`);
            return null;
          }
          
          // Extract the 7 required fields (only for conversions with emails)
          return extractRequiredConversionFields(parsed, key);
        }
      }
    } catch (error) {
      // Skip errors to keep processing
    }
    return null;
  });
  
  const results = await Promise.all(batchPromises);
  return results.filter(result => result !== null);
}

// Extract the 7 required fields from conversion data (only for conversions with valid emails)
function extractRequiredConversionFields(rawConversion, redisKey) {
  // Additional email validation
  const email = rawConversion.email;
  if (!email || email === 'unknown' || email === '' || email === null || email.trim() === '') {
    console.log(`⚠️ Invalid email detected in conversion: "${email}"`);
    return null; // Don't process conversions without valid emails
  }
  
  // Validate email format (basic check)
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email.trim())) {
    console.log(`⚠️ Invalid email format detected: "${email}"`);
    return null; // Don't process conversions with invalid email formats
  }
  
  // Parse unique IPs from various formats
  const parseUniqueIPs = (uniqueIPsField) => {
    if (!uniqueIPsField) return [];
    
    if (Array.isArray(uniqueIPsField)) {
      return uniqueIPsField.filter(ip => ip && ip !== 'unknown');
    }
    
    if (typeof uniqueIPsField === 'string') {
      return uniqueIPsField.split(',')
        .map(ip => ip.trim())
        .filter(ip => ip && ip !== 'unknown' && ip !== 'null');
    }
    
    return [];
  };
  
  return {
    // Required fields for indexing (email is guaranteed to be valid)
    timestamp: rawConversion.timestamp,
    email: email.trim().toLowerCase(), // Normalize email for consistency
    conversion_ip: rawConversion.conversion_ip || rawConversion.CIP || null,
    primary_ip: rawConversion.primary_ip || rawConversion.PIP || null,
    unique_ips: parseUniqueIPs(rawConversion.unique_ips),
    ssid: rawConversion.ssid || rawConversion.session_id || null,
    landing_page: rawConversion.landing_page || null,
    
    // Additional useful fields
    order_total: parseFloat(rawConversion.order_total) || 0,
    order_id: rawConversion.order_id || null,
    source: rawConversion.source || null,
    attribution_found: rawConversion.attribution_found || false,
    
    // Metadata
    redis_key: redisKey,
    extracted_at: new Date().toISOString()
  };
}

// Build conversion indexes for fast lookup (v2)
async function buildConversionIndexes(redis, conversions, maxTime) {
  const indexStartTime = Date.now();
  let indexesCreated = 0;
  
  console.log(`🏗️ Building conversion indexes for ${conversions.length} conversions...`);
  
  // Group conversions for efficient indexing
  const emailGroups = new Map();
  const ipGroups = new Map();
  const sessionGroups = new Map();
  const dateGroups = new Map();
  
  // Group conversions by various fields
  conversions.forEach(conv => {
    // Group by email (email is guaranteed to be valid since we filtered during extraction)
    if (conv.email) {
      const emailKey = conv.email; // Already normalized to lowercase during extraction
      if (!emailGroups.has(emailKey)) {
        emailGroups.set(emailKey, []);
      }
      emailGroups.get(emailKey).push(conv);
    }
    
    // Group by IPs
    const ips = [conv.conversion_ip, conv.primary_ip, ...(conv.unique_ips || [])].filter(Boolean);
    ips.forEach(ip => {
      const encodedIP = encodeIPForKey(ip);
      if (!ipGroups.has(encodedIP)) {
        ipGroups.set(encodedIP, []);
      }
      ipGroups.get(encodedIP).push(conv);
    });
    
    // Group by session
    if (conv.ssid) {
      if (!sessionGroups.has(conv.ssid)) {
        sessionGroups.set(conv.ssid, []);
      }
      sessionGroups.get(conv.ssid).push(conv);
    }
    
    // Group by date
    if (conv.timestamp) {
      const date = new Date(conv.timestamp).toISOString().split('T')[0]; // YYYY-MM-DD
      if (!dateGroups.has(date)) {
        dateGroups.set(date, []);
      }
      dateGroups.get(date).push(conv);
    }
  });
  
  console.log(`📊 Grouped conversions: ${emailGroups.size} emails, ${ipGroups.size} IPs, ${sessionGroups.size} sessions, ${dateGroups.size} dates`);
  
  try {
    // Build email indexes (v2)
    indexesCreated += await buildEmailIndexes(redis, emailGroups, Math.floor(maxTime * 0.3));
    
    // Build IP indexes (v2)
    if (Date.now() - indexStartTime < maxTime * 0.7) {
      indexesCreated += await buildIPIndexes(redis, ipGroups, Math.floor(maxTime * 0.3));
    }
    
    // Build session indexes (v2)
    if (Date.now() - indexStartTime < maxTime * 0.8) {
      indexesCreated += await buildSessionIndexes(redis, sessionGroups, Math.floor(maxTime * 0.2));
    }
    
    // Build date indexes (v2)
    if (Date.now() - indexStartTime < maxTime * 0.9) {
      indexesCreated += await buildDateIndexes(redis, dateGroups, Math.floor(maxTime * 0.1));
    }
    
  } catch (error) {
    console.error('❌ Index building error:', error);
  }
  
  console.log(`✅ Built ${indexesCreated} conversion indexes in ${Date.now() - indexStartTime}ms`);
  
  return { indexes_created: indexesCreated };
}

// Build email-based conversion indexes (v2)
async function buildEmailIndexes(redis, emailGroups, maxTime) {
  const startTime = Date.now();
  const emails = Array.from(emailGroups.entries());
  const batchSize = 20;
  let created = 0;
  
  for (let i = 0; i < emails.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = emails.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([email, conversions]) => {
      try {
        const emailKey = `conversion_index_v1_email:${encodeURIComponent(email)}`;
        
        const indexData = {
          email: email,
          conversion_count: conversions.length,
          conversions: conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)),
          latest_conversion: conversions[0].timestamp,
          total_revenue: conversions.reduce((sum, conv) => sum + conv.order_total, 0),
          created_at: new Date().toISOString(),
          index_type: 'email_conversions'
        };
        
        await redis(`setex/${emailKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 1500); // 30 days
        return 1;
        
      } catch (error) {
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} email conversion indexes`);
  return created;
}

// Build IP-based conversion indexes (v2)
async function buildIPIndexes(redis, ipGroups, maxTime) {
  const startTime = Date.now();
  const ips = Array.from(ipGroups.entries());
  const batchSize = 30;
  let created = 0;
  
  for (let i = 0; i < ips.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = ips.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([encodedIP, conversions]) => {
      try {
        const ipKey = `conversion_index_v1_ip:${encodedIP}`;
        
        const indexData = {
          ip_address: conversions[0].conversion_ip || conversions[0].primary_ip,
          conversion_count: conversions.length,
          conversions: conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)),
          latest_conversion: conversions[0].timestamp,
          unique_emails: [...new Set(conversions.map(c => c.email))],
          total_revenue: conversions.reduce((sum, conv) => sum + conv.order_total, 0),
          created_at: new Date().toISOString(),
          index_type: 'ip_conversions'
        };
        
        await redis(`setex/${ipKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 1500); // 30 days
        return 1;
        
      } catch (error) {
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} IP conversion indexes`);
  return created;
}

// Build session-based conversion indexes (v2)
async function buildSessionIndexes(redis, sessionGroups, maxTime) {
  const startTime = Date.now();
  const sessions = Array.from(sessionGroups.entries());
  const batchSize = 30;
  let created = 0;
  
  for (let i = 0; i < sessions.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = sessions.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([sessionId, conversions]) => {
      try {
        const sessionKey = `conversion_index_v1_session:${sessionId}`;
        
        const indexData = {
          session_id: sessionId,
          conversion_count: conversions.length,
          conversions: conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)),
          latest_conversion: conversions[0].timestamp,
          unique_emails: [...new Set(conversions.map(c => c.email))],
          total_revenue: conversions.reduce((sum, conv) => sum + conv.order_total, 0),
          created_at: new Date().toISOString(),
          index_type: 'session_conversions'
        };
        
        await redis(`setex/${sessionKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 1500); // 30 days
        return 1;
        
      } catch (error) {
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} session conversion indexes`);
  return created;
}

// Build date-based conversion indexes (v2)
async function buildDateIndexes(redis, dateGroups, maxTime) {
  const startTime = Date.now();
  const dates = Array.from(dateGroups.entries());
  const batchSize = 20;
  let created = 0;
  
  for (let i = 0; i < dates.length; i += batchSize) {
    if (Date.now() - startTime > maxTime) break;
    
    const batch = dates.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ([date, conversions]) => {
      try {
        const dateKey = `conversion_index_v1_date:${date}`;
        
        const indexData = {
          date: date,
          conversion_count: conversions.length,
          conversions: conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)),
          unique_emails: [...new Set(conversions.map(c => c.email))],
          unique_ips: [...new Set(conversions.flatMap(c => [c.conversion_ip, c.primary_ip, ...(c.unique_ips || [])]).filter(Boolean))],
          total_revenue: conversions.reduce((sum, conv) => sum + conv.order_total, 0),
          created_at: new Date().toISOString(),
          index_type: 'date_conversions'
        };
        
        await redis(`setex/${dateKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 1500); // 30 days
        return 1;
        
      } catch (error) {
        return 0;
      }
    });
    
    const results = await Promise.all(batchPromises);
    created += results.reduce((sum, result) => sum + result, 0);
  }
  
  console.log(`✅ Created ${created} date conversion indexes`);
  return created;
}

// Utility function for encoding IPs
function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_').replace(/\./g, '_');
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
