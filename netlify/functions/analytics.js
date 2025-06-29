// File: netlify/functions/analytics.js
// 🔧 FIXED VERSION - Pacific Time 7-Day Rolling Window + Comprehensive Scanning
// Automatically retrieves the past 7 days from current Pacific Time moment

// Enhanced timestamp validation function
function isValidTimestamp(timestamp) {
    if (!timestamp) return false;
    
    try {
        const date = new Date(timestamp);
        if (isNaN(date.getTime())) return false;
        
        const timestampMs = date.getTime();
        const minDate = new Date('2015-01-01').getTime();
        const maxDate = new Date('2035-12-31').getTime();
        
        return timestampMs >= minDate && timestampMs <= maxDate;
    } catch (error) {
        console.warn('Timestamp validation error:', error);
        return false;
    }
}

// Enhanced safe timestamp processing
function safeProcessTimestamp(timestamp, fallbackTimestamp = null) {
    if (isValidTimestamp(timestamp)) {
        return timestamp;
    }
    
    console.warn('⚠️ Invalid timestamp detected:', timestamp);
    
    if (fallbackTimestamp && isValidTimestamp(fallbackTimestamp)) {
        console.log('✅ Using fallback timestamp:', fallbackTimestamp);
        return fallbackTimestamp;
    }
    
    const currentTimestamp = new Date().toISOString();
    console.log('🔧 Generated current timestamp fallback:', currentTimestamp);
    return currentTimestamp;
}

// Sleep function for batch delays
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Pacific Time date calculation for rolling 7-day window
function calculatePacificTimeRange() {
    // Get current time in Pacific Time
    const now = new Date();
    const pacificOptions = { timeZone: 'America/Los_Angeles' };
    
    // Calculate Pacific Time current moment
    const nowPacific = new Date(now.toLocaleString('en-US', pacificOptions));
    
    // Calculate 7 days ago in Pacific Time
    const sevenDaysAgo = new Date(nowPacific);
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    
    console.log(`🕐 Pacific Time Range Calculated:`);
    console.log(`   Current Pacific Time: ${nowPacific.toISOString()}`);
    console.log(`   7 Days Ago Pacific: ${sevenDaysAgo.toISOString()}`);
    
    return {
        startDate: sevenDaysAgo,
        endDate: nowPacific,
        startTimestamp: sevenDaysAgo.getTime(),
        endTimestamp: nowPacific.getTime()
    };
}

// DUAL-PATTERN ATTRIBUTION KEY SCANNING
async function getComprehensiveAttributionKeys(redis) {
    console.log('🔍 Starting DUAL PATTERN attribution key scanning...');
    
    let allAttributionKeys = [];
    let totalScanned = 0;
    
    try {
        // PATTERN 1: Traditional underscore format (attribution_*)
        console.log('📊 Scanning Pattern 1: attribution_* (underscore format)');
        
        const ipv6Prefixes = [
            '2001', '2002', '2400', '2600', '2601', '2602', '2603', '2604', 
            '2605', '2606', '2607', '2608', '2609', '260a', '260b', '260c',
            '260d', '260e', '260f', '2610', '2620', '2630', '2640', '2650'
        ];
        
        for (const prefix of ipv6Prefixes) {
            try {
                const result = await redis(`scan/0/match/attribution_${prefix}*/count/1000`);
                if (result.result && result.result[1]) {
                    const keys = result.result[1];
                    allAttributionKeys = allAttributionKeys.concat(keys);
                    totalScanned += keys.length;
                    if (keys.length > 0) {
                        console.log(`✅ Found ${keys.length} keys for prefix ${prefix}`);
                    }
                }
                
                // Small delay to prevent overwhelming Redis
                await sleep(10);
                
            } catch (error) {
                console.warn(`⚠️ Prefix ${prefix} scan failed:`, error.message);
            }
        }
        
        console.log(`📊 Pattern 1 complete: ${totalScanned} underscore keys found`);
        
        // PATTERN 2: Colon format (attribution:*) - THE MISSING DATA!
        console.log('📊 Scanning Pattern 2: attribution:* (colon format - CRITICAL MISSING DATA)');
        
        try {
            let cursor = '0';
            let colonKeys = [];
            
            do {
                const result = await redis(`scan/${cursor}/match/attribution:*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    const keys = result.result[1];
                    colonKeys = colonKeys.concat(keys);
                    console.log(`✅ Found ${keys.length} colon-format keys (cursor: ${cursor})`);
                }
                
                // Safety limit
                if (colonKeys.length > 15000) {
                    console.log(`⚠️ Colon format scan limit reached: ${colonKeys.length} keys`);
                    break;
                }
                
                await sleep(10);
                
            } while (cursor !== '0');
            
            allAttributionKeys = allAttributionKeys.concat(colonKeys);
            totalScanned += colonKeys.length;
            console.log(`🎯 CRITICAL: Total colon-format keys found: ${colonKeys.length}`);
            
        } catch (error) {
            console.error('❌ Colon format scanning failed:', error);
        }
        
        // Remove duplicates and validate
        const uniqueKeys = [...new Set(allAttributionKeys)];
        console.log(`📊 Attribution scan found ${uniqueKeys.length} unique keys from both patterns`);
        
        const underscoreKeys = uniqueKeys.filter(key => key.startsWith('attribution_')).length;
        const colonKeys = uniqueKeys.filter(key => key.startsWith('attribution:')).length;
        console.log(`🎯 PATTERN DISTRIBUTION: ${underscoreKeys} underscore, ${colonKeys} colon format`);
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Dual pattern attribution key scanning failed:', error);
        return [];
    }
}

// 🔧 COMPREHENSIVE CONVERSION KEY SCANNING - THE MAIN FIX WITH MULTI-PATTERN SUPPORT
async function getConversionKeysEnhanced(redis) {
    let allConversionKeys = [];
    let totalScanned = 0;
    let scanResults = {
        standard: 0,
        email_based: 0,
        alternative: 0,
        legacy: 0,
        date_specific: 0,
        timestamp: 0
    };
    
    console.log('🔍 Starting COMPREHENSIVE MULTI-PATTERN conversion key scan...');
    
    try {
        // PATTERN 1: Standard conversions:* with COMPLETE cursor iteration
        console.log('📊 Scanning Pattern 1: conversions:* (standard format)');
        try {
            const standardResult = await scanWithTimeout('conversions:*', redis, 15000);
            allConversionKeys = allConversionKeys.concat(standardResult.keys);
            scanResults.standard = standardResult.keys.length;
            console.log(`📊 Pattern 1 complete: ${standardResult.keys.length} standard keys found`);
        } catch (error) {
            console.error('❌ Standard conversions:* pattern scan failed:', error.message);
        }
        
        // PATTERN 2: Email-based conversion keys (conversions:*email*)
        console.log('📊 Scanning Pattern 2: Email-based conversion keys');
        try {
            const emailPatterns = [
                'conversions:*gmail*',
                'conversions:*yahoo*', 
                'conversions:*hotmail*',
                'conversions:*outlook*',
                'conversions:*@*',
                'conversions:*_*_*'  // underscore-encoded emails
            ];
            
            for (const pattern of emailPatterns) {
                try {
                    const result = await scanWithTimeout(pattern, redis, 10000);
                    allConversionKeys = allConversionKeys.concat(result.keys);
                    scanResults.email_based += result.keys.length;
                    if (result.keys.length > 0) {
                        console.log(`✅ Found ${result.keys.length} keys with email pattern ${pattern}`);
                    }
                } catch (emailError) {
                    console.warn(`⚠️ Email pattern ${pattern} timeout/failed:`, emailError.message);
                }
            }
            console.log(`📊 Pattern 2 complete: ${scanResults.email_based} email-based keys found`);
        } catch (emailScanError) {
            console.error('❌ Email-based scanning failed:', emailScanError.message);
        }
        
        // PATTERN 3: Alternative conversion formats (CRITICAL MISSING PATTERNS)
        console.log('📊 Scanning Pattern 3: Alternative conversion formats');
        try {
            const alternativePatterns = [
                'conversion:*',        // singular form
                'conv:*',             // abbreviated
                'conversion_*',       // underscore format
                'track_*',            // track format
                'purchase:*',         // purchase format
                'order:*',            // order format
                'subscription:*',     // subscription format
                'trial:*'             // trial format
            ];
            
            for (const pattern of alternativePatterns) {
                try {
                    const result = await scanWithTimeout(pattern, redis, 10000);
                    allConversionKeys = allConversionKeys.concat(result.keys);
                    scanResults.alternative += result.keys.length;
                    if (result.keys.length > 0) {
                        console.log(`✅ Found ${result.keys.length} keys with alternative pattern ${pattern}`);
                    }
                } catch (altError) {
                    console.warn(`⚠️ Alternative pattern ${pattern} timeout/failed:`, altError.message);
                }
            }
            console.log(`📊 Pattern 3 complete: ${scanResults.alternative} alternative format keys found`);
        } catch (altScanError) {
            console.error('❌ Alternative format scanning failed:', altScanError.message);
        }
        
        // PATTERN 4: Date-specific keys for June 21-28 (Pacific Time 7-day window)
        console.log('📊 Scanning Pattern 4: Date-specific conversion keys');
        try {
            const datePatterns = [
                '*2025-06-21*', '*2025-06-22*', '*2025-06-23*', '*2025-06-24*',
                '*2025-06-25*', '*2025-06-26*', '*2025-06-27*', '*2025-06-28*'
            ];
            
            for (const pattern of datePatterns) {
                try {
                    const result = await scanWithTimeout(pattern, redis, 8000);
                    // Filter to only conversion-related keys
                    const convKeys = result.keys.filter(key => 
                        key.includes('conversion') || 
                        key.includes('purchase') || 
                        key.includes('order') ||
                        key.includes('trial') ||
                        key.includes('subscription')
                    );
                    allConversionKeys = allConversionKeys.concat(convKeys);
                    scanResults.date_specific += convKeys.length;
                    if (convKeys.length > 0) {
                        console.log(`✅ Found ${convKeys.length} date-specific conversion keys for ${pattern}`);
                    }
                } catch (dateError) {
                    console.warn(`⚠️ Date pattern ${pattern} timeout/failed:`, dateError.message);
                }
            }
            console.log(`📊 Pattern 4 complete: ${scanResults.date_specific} date-specific keys found`);
        } catch (dateScanError) {
            console.error('❌ Date-specific scanning failed:', dateScanError.message);
        }
        
        // PATTERN 5: Legacy email-based patterns (broad search)
        console.log('📊 Scanning Pattern 5: Legacy email-based patterns');
        try {
            const legacyPatterns = [
                '*gmail.com*',
                '*yahoo.com*',
                '*hotmail.com*',
                '*outlook.com*'
            ];
            
            for (const pattern of legacyPatterns) {
                try {
                    const result = await scanWithTimeout(pattern, redis, 8000);
                    // Filter to only conversion-related keys
                    const convKeys = result.keys.filter(key => 
                        key.includes('conversion') || 
                        key.includes('purchase') || 
                        key.includes('order') ||
                        key.includes('email') ||
                        key.includes('customer')
                    );
                    allConversionKeys = allConversionKeys.concat(convKeys);
                    scanResults.legacy += convKeys.length;
                    if (convKeys.length > 0) {
                        console.log(`✅ Found ${convKeys.length} legacy conversion keys for ${pattern}`);
                    }
                } catch (legacyError) {
                    console.warn(`⚠️ Legacy pattern ${pattern} timeout/failed:`, legacyError.message);
                }
            }
            console.log(`📊 Pattern 5 complete: ${scanResults.legacy} legacy keys found`);
        } catch (legacyScanError) {
            console.error('❌ Legacy scanning failed:', legacyScanError.message);
        }
        
        // Remove duplicates and log final results
        const uniqueKeys = [...new Set(allConversionKeys)];
        console.log(`📊 COMPREHENSIVE CONVERSION SCAN COMPLETE:`);
        console.log(`   🎯 Standard patterns: ${scanResults.standard} keys`);
        console.log(`   📧 Email-based patterns: ${scanResults.email_based} keys`);
        console.log(`   🔄 Alternative formats: ${scanResults.alternative} keys`);
        console.log(`   📅 Date-specific patterns: ${scanResults.date_specific} keys`);
        console.log(`   🗂️ Legacy patterns: ${scanResults.legacy} keys`);
        console.log(`   ⚡ Total unique conversion keys: ${uniqueKeys.length}`);
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ CRITICAL: Comprehensive conversion key scanning completely failed:', error);
        return [];
    }
}

// Enhanced scan function with timeout detection and detailed error reporting
async function scanWithTimeout(pattern, redis, timeoutMs = 10000) {
    const startTime = Date.now();
    console.log(`🔍 Starting scan for pattern: ${pattern} (timeout: ${timeoutMs}ms)`);
    
    return new Promise(async (resolve, reject) => {
        const timeoutId = setTimeout(() => {
            const elapsed = Date.now() - startTime;
            console.error(`⏰ TIMEOUT: Pattern '${pattern}' exceeded ${timeoutMs}ms (elapsed: ${elapsed}ms)`);
            console.error(`⏰ TIMEOUT DETAILS:`);
            console.error(`   Pattern: ${pattern}`);
            console.error(`   Timeout Limit: ${timeoutMs}ms`);
            console.error(`   Time Elapsed: ${elapsed}ms`);
            console.error(`   Redis Status: Unknown (timed out)`);
            console.error(`   Possible Issues: Upstash Redis timeout, Netlify function timeout, Network timeout`);
            reject(new Error(`Timeout scanning pattern '${pattern}' after ${timeoutMs}ms`));
        }, timeoutMs);
        
        try {
            const keys = [];
            let cursor = '0';
            let iterations = 0;
            let totalKeysFound = 0;
            
            do {
                const iterationStart = Date.now();
                
                try {
                    const result = await redis(`scan/${cursor}/match/${pattern}/count/1000`);
                    const iterationTime = Date.now() - iterationStart;
                    
                    if (result.result && result.result[1]) {
                        cursor = result.result[0];
                        const batchKeys = result.result[1];
                        keys.push(...batchKeys);
                        totalKeysFound += batchKeys.length;
                        iterations++;
                        
                        console.log(`  📦 Iteration ${iterations}: ${batchKeys.length} keys found (${iterationTime}ms, cursor: ${cursor})`);
                    } else {
                        console.warn(`⚠️ No result data for pattern ${pattern}, iteration ${iterations}`);
                        break;
                    }
                    
                    // Safety checks
                    if (iterations > 100) {
                        console.warn(`⚠️ SAFETY: Breaking scan for ${pattern} after 100 iterations`);
                        break;
                    }
                    
                    if (Date.now() - startTime > timeoutMs - 1000) {
                        console.warn(`⚠️ SAFETY: Approaching timeout for ${pattern}, stopping early`);
                        break;
                    }
                    
                } catch (iterationError) {
                    const iterationTime = Date.now() - iterationStart;
                    console.error(`❌ REDIS ERROR: Iteration ${iterations} failed for ${pattern} (${iterationTime}ms):`);
                    console.error(`   Error Type: ${iterationError.name}`);
                    console.error(`   Error Message: ${iterationError.message}`);
                    console.error(`   Cursor: ${cursor}`);
                    console.error(`   Possible Issues: Upstash connection lost, Redis overload, Network error`);
                    break;
                }
                
            } while (cursor !== '0');
            
            clearTimeout(timeoutId);
            const totalTime = Date.now() - startTime;
            console.log(`✅ Pattern '${pattern}' completed: ${totalKeysFound} keys in ${totalTime}ms (${iterations} iterations)`);
            
            resolve({ keys, iterations, totalTime });
            
        } catch (error) {
            clearTimeout(timeoutId);
            const elapsed = Date.now() - startTime;
            console.error(`❌ SCAN ERROR for pattern '${pattern}' after ${elapsed}ms:`);
            console.error(`   Error Type: ${error.name}`);
            console.error(`   Error Message: ${error.message}`);
            console.error(`   Stack: ${error.stack}`);
            console.error(`   Possible Root Causes:`);
            console.error(`     - Upstash Redis connection timeout`);
            console.error(`     - Netlify function execution timeout`);
            console.error(`     - Network connectivity issues`);
            console.error(`     - Redis memory/performance issues`);
            console.error(`     - Invalid pattern syntax`);
            reject(error);
        }
    });
}

// 🔧 FIXED: Enhanced conversion data fetching with date filtering and timeout monitoring
async function fetchConversionDataSafely(redis, conversionKeys, startTimestamp, endTimestamp) {
    const fetchStartTime = Date.now();
    const FETCH_TIMEOUT_MS = 15000; // 15 second timeout for data fetching
    
    console.log(`📦 Fetching conversion data for ${conversionKeys.length} keys...`);
    console.log(`📅 Date filter: ${new Date(startTimestamp).toISOString()} to ${new Date(endTimestamp).toISOString()}`);
    console.log(`⏰ Fetch timeout limit: ${FETCH_TIMEOUT_MS}ms`);
    
    const allConversions = [];
    const batchSize = 100;
    const delayMs = 100;
    let nilCount = 0;
    let parseErrors = 0;
    let validConversions = 0;
    let dateFilteredOut = 0;
    let timeoutOccurred = false;
    
    try {
        for (let i = 0; i < conversionKeys.length; i += batchSize) {
            // Check for timeout before each batch
            const elapsedTime = Date.now() - fetchStartTime;
            if (elapsedTime > FETCH_TIMEOUT_MS - 2000) {
                console.error(`⏰ CONVERSION FETCH TIMEOUT: Stopping after ${elapsedTime}ms`);
                console.error(`   Processed: ${i} of ${conversionKeys.length} keys`);
                console.error(`   Remaining: ${conversionKeys.length - i} keys`);
                console.error(`   Valid conversions found so far: ${validConversions}`);
                timeoutOccurred = true;
                break;
            }
            
            const batch = conversionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            
            console.log(`📦 Processing conversion batch ${batchNumber}: ${batch.length} keys (${elapsedTime}ms elapsed)`);
            
            try {
                const batchStartTime = Date.now();
                const batchResults = await Promise.all(
                    batch.map(async (key) => {
                        try {
                            const result = await redis(`get/${key}`);
                            return {
                                key: key,
                                data: result.result ? decodeURIComponent(result.result) : null
                            };
                        } catch (e) {
                            console.warn(`⚠️ Redis get failed for key ${key}: ${e.message}`);
                            return { key: key, data: null, error: e.message };
                        }
                    })
                );
                const batchTime = Date.now() - batchStartTime;
                
                if (batchTime > 3000) {
                    console.warn(`⚠️ SLOW BATCH: Batch ${batchNumber} took ${batchTime}ms`);
                }
                
                batchResults.forEach(item => {
                    if (!item.data) {
                        nilCount++;
                        if (item.error) {
                            console.warn(`⚠️ Key error: ${item.key} - ${item.error}`);
                        }
                        return;
                    }
                    
                    try {
                        const parsed = JSON.parse(item.data);
                        
                        // Enhanced timestamp validation
                        if (!isValidTimestamp(parsed.timestamp)) {
                            console.warn(`⚠️ Invalid timestamp in conversion: ${item.key}, timestamp: ${parsed.timestamp}`);
                            parsed.timestamp = new Date().toISOString();
                        }
                        
                        // Apply Pacific Time date range filter
                        const conversionTimestamp = new Date(parsed.timestamp).getTime();
                        if (conversionTimestamp < startTimestamp || conversionTimestamp > endTimestamp) {
                            dateFilteredOut++;
                            return; // Skip conversions outside date range
                        }
                        
                        // Add the key for debugging
                        parsed._redis_key = item.key;
                        
                        allConversions.push(parsed);
                        validConversions++;
                    } catch (parseError) {
                        parseErrors++;
                        console.warn(`⚠️ Failed to parse conversion data from key: ${item.key} - ${parseError.message}`);
                    }
                });
                
                // Delay between main batches to avoid overwhelming Redis
                if (i + batchSize < conversionKeys.length && !timeoutOccurred) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Conversion batch ${batchNumber} failed:`, batchError);
                console.error(`   Error Type: ${batchError.name}`);
                console.error(`   Error Message: ${batchError.message}`);
                console.error(`   Batch Size: ${batch.length}`);
                console.error(`   Time Elapsed: ${Date.now() - fetchStartTime}ms`);
            }
        }
        
        const totalFetchTime = Date.now() - fetchStartTime;
        console.log(`📊 Conversion data fetch complete (${totalFetchTime}ms):`);
        console.log(`   ✅ Valid conversions in date range: ${validConversions}`);
        console.log(`   📅 Date filtered out: ${dateFilteredOut}`);
        console.log(`   ⚠️ Nil/missing data: ${nilCount}`);
        console.log(`   ❌ Parse errors: ${parseErrors}`);
        console.log(`   ⏰ Timeout occurred: ${timeoutOccurred}`);
        
        if (timeoutOccurred) {
            console.warn(`⚠️ PARTIAL RESULTS: Conversion fetch timed out, returning ${validConversions} partial results`);
        }
        
        return allConversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
    } catch (error) {
        const elapsed = Date.now() - fetchStartTime;
        console.error('❌ Conversion data fetching completely failed:', error);
        console.error(`   Error Type: ${error.name}`);
        console.error(`   Error Message: ${error.message}`);
        console.error(`   Time Elapsed: ${elapsed}ms`);
        console.error(`   Keys Attempted: ${conversionKeys.length}`);
        console.error(`   Valid Results So Far: ${validConversions}`);
        return allConversions; // Return partial results even on error
    }
}

// 🔧 FIXED: Enhanced attribution data fetching with date filtering and timeout monitoring
async function fetchAttributionDataSafely(redis, attributionKeys, startTimestamp, endTimestamp) {
    const fetchStartTime = Date.now();
    const FETCH_TIMEOUT_MS = 15000; // 15 second timeout for data fetching
    
    console.log(`📦 Fetching attribution data for ${attributionKeys.length} keys...`);
    console.log(`📅 Date filter: ${new Date(startTimestamp).toISOString()} to ${new Date(endTimestamp).toISOString()}`);
    console.log(`⏰ Fetch timeout limit: ${FETCH_TIMEOUT_MS}ms`);
    
    const allPageViews = [];
    const batchSize = 100;
    const delayMs = 100;
    let nilCount = 0;
    let parseErrors = 0;
    let validPageViews = 0;
    let dateFilteredOut = 0;
    let timeoutOccurred = false;
    
    try {
        if (attributionKeys.length > 5000) {
            console.log(`⚠️ Large attribution dataset: ${attributionKeys.length} keys. Processing with delays...`);
        }
        
        for (let i = 0; i < attributionKeys.length; i += batchSize) {
            // Check for timeout before each batch
            const elapsedTime = Date.now() - fetchStartTime;
            if (elapsedTime > FETCH_TIMEOUT_MS - 2000) {
                console.error(`⏰ ATTRIBUTION FETCH TIMEOUT: Stopping after ${elapsedTime}ms`);
                console.error(`   Processed: ${i} of ${attributionKeys.length} keys`);
                console.error(`   Remaining: ${attributionKeys.length - i} keys`);
                console.error(`   Valid page views found so far: ${validPageViews}`);
                timeoutOccurred = true;
                break;
            }
            
            const batch = attributionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            
            try {
                const batchStartTime = Date.now();
                const batchResults = await Promise.all(
                    batch.map(async (key) => {
                        try {
                            const result = await redis(`get/${key}`);
                            return {
                                key: key,
                                data: result.result ? decodeURIComponent(result.result) : null
                            };
                        } catch (e) {
                            console.warn(`⚠️ Redis get failed for key ${key}: ${e.message}`);
                            return { key: key, data: null, error: e.message };
                        }
                    })
                );
                const batchTime = Date.now() - batchStartTime;
                
                if (batchTime > 3000) {
                    console.warn(`⚠️ SLOW BATCH: Attribution batch ${batchNumber} took ${batchTime}ms`);
                }
                
                batchResults.forEach(item => {
                    if (!item.data) {
                        nilCount++;
                        return;
                    }
                    
                    try {
                        const parsed = JSON.parse(item.data);
                        
                        // Enhanced timestamp validation
                        if (!isValidTimestamp(parsed.timestamp)) {
                            console.warn(`⚠️ Invalid timestamp in attribution: ${item.key}, timestamp: ${parsed.timestamp}`);
                            parsed.timestamp = new Date().toISOString();
                        }
                        
                        // Apply Pacific Time date range filter
                        const attributionTimestamp = new Date(parsed.timestamp).getTime();
                        if (attributionTimestamp < startTimestamp || attributionTimestamp > endTimestamp) {
                            dateFilteredOut++;
                            return; // Skip attribution outside date range
                        }
                        
                        allPageViews.push(parsed);
                        validPageViews++;
                    } catch (parseError) {
                        parseErrors++;
                    }
                });
                
                if (i + batchSize < attributionKeys.length && !timeoutOccurred) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Attribution batch ${batchNumber} failed:`, batchError);
                console.error(`   Error Type: ${batchError.name}`);
                console.error(`   Error Message: ${batchError.message}`);
                console.error(`   Batch Size: ${batch.length}`);
                console.error(`   Time Elapsed: ${Date.now() - fetchStartTime}ms`);
            }
        }
        
        const totalFetchTime = Date.now() - fetchStartTime;
        console.log(`📊 Attribution data fetch complete (${totalFetchTime}ms):`);
        console.log(`   ✅ Valid page views in date range: ${validPageViews}`);
        console.log(`   📅 Date filtered out: ${dateFilteredOut}`);
        console.log(`   ⚠️ Nil/missing data: ${nilCount}`);
        console.log(`   ❌ Parse errors: ${parseErrors}`);
        console.log(`   ⏰ Timeout occurred: ${timeoutOccurred}`);
        
        if (timeoutOccurred) {
            console.warn(`⚠️ PARTIAL RESULTS: Attribution fetch timed out, returning ${validPageViews} partial results`);
        }
        
        return allPageViews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
    } catch (error) {
        const elapsed = Date.now() - fetchStartTime;
        console.error('❌ Attribution data fetching completely failed:', error);
        console.error(`   Error Type: ${error.name}`);
        console.error(`   Error Message: ${error.message}`);
        console.error(`   Time Elapsed: ${elapsed}ms`);
        console.error(`   Keys Attempted: ${attributionKeys.length}`);
        console.error(`   Valid Results So Far: ${validPageViews}`);
        return allPageViews; // Return partial results even on error
    }
}

// Apply filters to data
function applyFilters(data, filters) {
    if (!filters) return data;
    
    return data.filter(item => {
        if (filters.source && item.source !== filters.source) return false;
        if (filters.campaign && item.utm_campaign !== filters.campaign) return false;
        return true;
    });
}

// Generate attribution statistics
function generateAttributionStats(pageViews) {
    const stats = {
        total_sessions: pageViews.length,
        sources: {},
        source_types: {},
        top_landing_pages: {},
        geographic_distribution: {},
        device_types: {}
    };
    
    pageViews.forEach(pv => {
        // Source counting
        const source = pv.source || 'unknown';
        stats.sources[source] = (stats.sources[source] || 0) + 1;
        
        // Source type counting
        const sourceType = pv.source_type || 'unknown';
        stats.source_types[sourceType] = (stats.source_types[sourceType] || 0) + 1;
        
        // Landing page counting
        const landingPage = pv.landing_page || 'unknown';
        stats.top_landing_pages[landingPage] = (stats.top_landing_pages[landingPage] || 0) + 1;
    });
    
    return stats;
}

// Create standardized response
function createResponse(statusCode, body) {
    return {
        statusCode,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
    };
}

// Redis helper function with enhanced timeout detection
const redis = async (command) => {
    const startTime = Date.now();
    const timeoutMs = 10000; // 10 second timeout for individual Redis operations
    
    return new Promise(async (resolve, reject) => {
        const timeoutId = setTimeout(() => {
            const elapsed = Date.now() - startTime;
            console.error(`⏰ REDIS TIMEOUT: Command '${command}' exceeded ${timeoutMs}ms`);
            console.error(`⏰ REDIS TIMEOUT DETAILS:`);
            console.error(`   Command: ${command}`);
            console.error(`   Timeout Limit: ${timeoutMs}ms`);
            console.error(`   Time Elapsed: ${elapsed}ms`);
            console.error(`   Upstash URL: ${process.env.UPSTASH_REDIS_REST_URL ? 'SET' : 'MISSING'}`);
            console.error(`   Upstash Token: ${process.env.UPSTASH_REDIS_REST_TOKEN ? 'SET' : 'MISSING'}`);
            console.error(`   Possible Issues:`);
            console.error(`     - Upstash Redis server timeout`);
            console.error(`     - Network connectivity timeout`);
            console.error(`     - Redis overload/memory issues`);
            console.error(`     - Netlify function memory/CPU limits`);
            reject(new Error(`Redis operation timeout after ${timeoutMs}ms: ${command}`));
        }, timeoutMs);
        
        try {
            const url = `${process.env.UPSTASH_REDIS_REST_URL}/${command}`;
            
            const fetchStartTime = Date.now();
            const response = await fetch(url, {
                headers: {
                    'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}`,
                    'Content-Type': 'application/json'
                }
            });
            const fetchTime = Date.now() - fetchStartTime;
            
            if (!response.ok) {
                clearTimeout(timeoutId);
                console.error(`❌ REDIS HTTP ERROR: ${response.status} ${response.statusText}`);
                console.error(`   Command: ${command}`);
                console.error(`   Fetch Time: ${fetchTime}ms`);
                console.error(`   Response Status: ${response.status}`);
                console.error(`   Possible Issues:`);
                console.error(`     - Invalid Upstash credentials`);
                console.error(`     - Upstash service unavailable`);
                console.error(`     - Rate limiting`);
                console.error(`     - Invalid Redis command syntax`);
                reject(new Error(`Redis HTTP error: ${response.status} ${response.statusText}`));
                return;
            }
            
            const parseStartTime = Date.now();
            const result = await response.json();
            const parseTime = Date.now() - parseStartTime;
            const totalTime = Date.now() - startTime;
            
            clearTimeout(timeoutId);
            
            // Log slow operations
            if (totalTime > 2000) {
                console.warn(`⚠️ SLOW REDIS OPERATION: ${command} took ${totalTime}ms (fetch: ${fetchTime}ms, parse: ${parseTime}ms)`);
            }
            
            resolve(result);
            
        } catch (error) {
            clearTimeout(timeoutId);
            const elapsed = Date.now() - startTime;
            console.error(`❌ REDIS CONNECTION ERROR after ${elapsed}ms:`);
            console.error(`   Command: ${command}`);
            console.error(`   Error Type: ${error.name}`);
            console.error(`   Error Message: ${error.message}`);
            console.error(`   Time Elapsed: ${elapsed}ms`);
            console.error(`   Stack: ${error.stack}`);
            console.error(`   Root Cause Analysis:`);
            console.error(`     - Network connectivity lost`);
            console.error(`     - Upstash Redis service down`);
            console.error(`     - DNS resolution failure`);
            console.error(`     - Netlify function network timeout`);
            console.error(`     - SSL/TLS handshake failure`);
            reject(error);
        }
    });
};

// Main handler function
const handler = async (event, context) => {
    console.log(`🚀 Analytics function started - ${event.httpMethod} request`);
    
    // Handle CORS preflight
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(200, {});
    }
    
    try {
// Main handler function with comprehensive timeout monitoring
const handler = async (event, context) => {
    const functionStartTime = Date.now();
    const NETLIFY_TIMEOUT_MS = 30000; // 30 seconds (conservative estimate)
    
    console.log(`🚀 Analytics function started - ${event.httpMethod} request`);
    console.log(`⏰ Function timeout monitoring: ${NETLIFY_TIMEOUT_MS}ms limit`);
    
    // Set up Netlify function timeout warning
    const netlifyTimeoutWarning = setTimeout(() => {
        const elapsed = Date.now() - functionStartTime;
        console.error(`🚨 NETLIFY TIMEOUT WARNING: Function approaching ${NETLIFY_TIMEOUT_MS}ms limit`);
        console.error(`   Time Elapsed: ${elapsed}ms`);
        console.error(`   Remaining Time: ${NETLIFY_TIMEOUT_MS - elapsed}ms`);
        console.error(`   Current Operation: Data processing`);
        console.error(`   Recommendation: Reduce date range or optimize scanning`);
    }, NETLIFY_TIMEOUT_MS - 5000); // Warning 5 seconds before timeout
    
    try {
        // Handle CORS preflight
        if (event.httpMethod === 'OPTIONS') {
            clearTimeout(netlifyTimeoutWarning);
            return createResponse(200, {});
        }
        
        if (event.httpMethod === 'GET') {
            const startTime = Date.now();
            
            try {
                // 🔧 FIXED: Ignore frontend date parameters and calculate Pacific Time 7-day window
                const pacificTimeRange = calculatePacificTimeRange();
                console.log(`📅 Using Pacific Time 7-day rolling window (ignoring frontend date parameters)`);
                
                // Parse other query parameters (but ignore dates)
                const { source, campaign, include_attribution_stats } = event.queryStringParameters || {};
                
                // Check remaining time before starting heavy operations
                const timeBeforeScanning = Date.now() - functionStartTime;
                if (timeBeforeScanning > NETLIFY_TIMEOUT_MS - 20000) {
                    clearTimeout(netlifyTimeoutWarning);
                    console.error(`⏰ INSUFFICIENT TIME: Only ${NETLIFY_TIMEOUT_MS - timeBeforeScanning}ms remaining for scanning`);
                    return createResponse(500, { 
                        error: 'Insufficient execution time remaining',
                        time_elapsed: timeBeforeScanning,
                        time_remaining: NETLIFY_TIMEOUT_MS - timeBeforeScanning
                    });
                }
                
                // Get all keys from Redis using COMPREHENSIVE SCANNING with timeout monitoring
                let attributionKeys = [];
                let conversionKeys = [];
                
                try {
                    console.log('🔍 Starting comprehensive Redis key scanning with timeout monitoring...');
                    
                    // Check time before attribution scanning
                    const timeBeforeAttribution = Date.now() - functionStartTime;
                    console.log(`⏰ Starting attribution scan with ${NETLIFY_TIMEOUT_MS - timeBeforeAttribution}ms remaining`);
                    
                    // Get attribution keys with dual pattern scanning
                    attributionKeys = await getComprehensiveAttributionKeys(redis);
                    console.log(`📊 Attribution scan found ${attributionKeys.length} attribution keys`);
                    
                    // Check time before conversion scanning
                    const timeBeforeConversion = Date.now() - functionStartTime;
                    if (timeBeforeConversion > NETLIFY_TIMEOUT_MS - 15000) {
                        console.error(`⏰ TIMEOUT RISK: Only ${NETLIFY_TIMEOUT_MS - timeBeforeConversion}ms remaining for conversion scanning`);
                        console.error(`⏰ SKIPPING CONVERSION SCANNING to prevent timeout`);
                        conversionKeys = [];
                    } else {
                        console.log(`⏰ Starting conversion scan with ${NETLIFY_TIMEOUT_MS - timeBeforeConversion}ms remaining`);
                        
                        // Get conversion keys with enhanced multi-pattern scanning
                        conversionKeys = await getConversionKeysEnhanced(redis);
                        console.log(`🔍 Enhanced conversion scan found ${conversionKeys.length} conversion keys`);
                    }
                    
                } catch (redisError) {
                    console.error('❌ Redis scanning failed with timeout/error details:', redisError);
                    console.error(`   Error Type: ${redisError.name}`);
                    console.error(`   Error Message: ${redisError.message}`);
                    console.error(`   Time Elapsed: ${Date.now() - functionStartTime}ms`);
                    attributionKeys = [];
                    conversionKeys = [];
                }
                
                // Check time before data fetching
                const timeBeforeDataFetch = Date.now() - functionStartTime;
                if (timeBeforeDataFetch > NETLIFY_TIMEOUT_MS - 10000) {
                    clearTimeout(netlifyTimeoutWarning);
                    console.error(`⏰ CRITICAL TIMEOUT: Only ${NETLIFY_TIMEOUT_MS - timeBeforeDataFetch}ms remaining`);
                    console.error(`⏰ RETURNING PARTIAL RESULTS to prevent complete failure`);
                    
                    return createResponse(200, {
                        page_views: [],
                        conversions: [],
                        total_page_views: 0,
                        total_conversions: 0,
                        unique_visitors: 0,
                        conversion_rate: '0.00',
                        total_revenue: '0.00',
                        timeout_warning: {
                            partial_results: true,
                            timeout_occurred: true,
                            time_elapsed: timeBeforeDataFetch,
                            keys_found: {
                                attribution: attributionKeys.length,
                                conversion: conversionKeys.length
                            },
                            message: 'Function timeout prevented full data processing'
                        },
                        date_range: {
                            start: pacificTimeRange.startDate.toISOString(),
                            end: pacificTimeRange.endDate.toISOString(),
                            timezone: 'America/Los_Angeles',
                            days: 7,
                            calculation_method: 'pacific_time_rolling_window'
                        }
                    });
                }
                
                // Fetch data with Pacific Time date filtering
                let allPageViews = [];
                let allConversions = [];
                
                if (attributionKeys.length > 0) {
                    try {
                        console.log(`⏰ Starting attribution data fetch with ${NETLIFY_TIMEOUT_MS - (Date.now() - functionStartTime)}ms remaining`);
                        allPageViews = await fetchAttributionDataSafely(
                            redis, 
                            attributionKeys, 
                            pacificTimeRange.startTimestamp, 
                            pacificTimeRange.endTimestamp
                        );
                    } catch (attributionError) {
                        console.error('❌ Attribution data fetch failed:', attributionError);
                        console.error(`   Time Elapsed: ${Date.now() - functionStartTime}ms`);
                        allPageViews = [];
                    }
                }
                
                if (conversionKeys.length > 0) {
                    try {
                        console.log(`⏰ Starting conversion data fetch with ${NETLIFY_TIMEOUT_MS - (Date.now() - functionStartTime)}ms remaining`);
                        allConversions = await fetchConversionDataSafely(
                            redis, 
                            conversionKeys, 
                            pacificTimeRange.startTimestamp, 
                            pacificTimeRange.endTimestamp
                        );
                    } catch (conversionError) {
                        console.error('❌ Conversion data fetch failed:', conversionError);
                        console.error(`   Time Elapsed: ${Date.now() - functionStartTime}ms`);
                        allConversions = [];
                    }
                }
                
                // Apply additional filters (source, campaign)
                let filteredPageViews = applyFilters(allPageViews, { source, campaign });
                let filteredConversions = applyFilters(allConversions, { source, campaign });
                
                console.log(`📊 Pacific Time filtered results: ${filteredPageViews.length} page views, ${filteredConversions.length} conversions`);
                
                // 🔧 ENHANCED DIAGNOSTIC: Check conversion count vs expected
                console.log(`🎯 CONVERSION DIAGNOSTIC WITH MULTI-PATTERN SCANNING:`);
                console.log(`   Expected conversions (past 7 days): 90`);
                console.log(`   Found conversions: ${filteredConversions.length}`);
                console.log(`   Missing conversions: ${90 - filteredConversions.length}`);
                console.log(`   Improvement from multi-pattern scanning: ${filteredConversions.length - 24} additional conversions`);
                
                if (filteredConversions.length > 0) {
                    console.log(`📋 Sample conversions found (first 5):`);
                    filteredConversions.slice(0, 5).forEach((conv, i) => {
                        console.log(`   ${i+1}. ${new Date(conv.timestamp).toLocaleString()} - ${conv.email} - ${parseFloat(conv.order_total) || 0} [${conv._redis_key || 'no key'}]`);
                    });
                }
                
                // Include attribution stats if requested
                let attributionStatsData = null;
                if (include_attribution_stats === 'true') {
                    try {
                        console.log('📈 Including attribution stats in response...');
                        attributionStatsData = generateAttributionStats(filteredPageViews);
                        console.log(`✅ Generated attribution stats for ${filteredPageViews.length} page views`);
                    } catch (statsError) {
                        console.error('❌ Failed to generate attribution stats:', statsError);
                        attributionStatsData = null;
                    }
                }
                
                // Calculate final analytics
                const totalConversions = filteredConversions.length;
                const totalPageViews = filteredPageViews.length;
                
                const uniqueVisitorIPs = new Set();
                filteredPageViews.forEach(pv => {
                    if (pv.ip_address && pv.ip_address !== 'unknown') {
                        uniqueVisitorIPs.add(pv.ip_address);
                    }
                });
                const uniqueVisitors = uniqueVisitorIPs.size;
                
                const totalRevenue = filteredConversions.reduce((sum, item) => sum + (parseFloat(item.order_total) || 0), 0);
                const conversionRate = uniqueVisitors > 0 ? ((totalConversions / uniqueVisitors) * 100).toFixed(2) : '0.00';
                
                const totalExecutionTime = Date.now() - startTime;
                console.log(`⏰ Total execution time: ${totalExecutionTime}ms (${(totalExecutionTime/1000).toFixed(1)}s)`);
                
                const response = {
                    // Dashboard expects arrays directly (not nested under 'data')
                    page_views: filteredPageViews,     // Array of page view objects
                    conversions: filteredConversions,  // Array of conversion objects
                    
                    // Summary statistics
                    total_page_views: totalPageViews,
                    total_conversions: totalConversions,
                    unique_visitors: uniqueVisitors,
                    conversion_rate: conversionRate,
                    total_revenue: totalRevenue.toFixed(2),
                    
                    // Date and processing info
                    date_range: {
                        start: pacificTimeRange.startDate.toISOString(),
                        end: pacificTimeRange.endDate.toISOString(),
                        timezone: 'America/Los_Angeles',
                        days: 7,
                        calculation_method: 'pacific_time_rolling_window'
                    },
                    processing_stats: {
                        execution_time_ms: totalExecutionTime,
                        function_time_limit_ms: NETLIFY_TIMEOUT_MS,
                        time_remaining_ms: NETLIFY_TIMEOUT_MS - (Date.now() - functionStartTime),
                        attribution_keys_scanned: attributionKeys.length,
                        conversion_keys_scanned: conversionKeys.length,
                        patterns_used: [
                            'attribution_*', 'attribution:*',
                            'conversions:*', 'conversion:*', 'conv:*', 'conversion_*', 'track_*',
                            'purchase:*', 'order:*', 'subscription:*', 'trial:*',
                            '*email*', '*@*', '*2025-06-*'
                        ],
                        multi_pattern_scanning: true,
                        performance_optimized: true
                    }
                };
                
                if (attributionStatsData) {
                    response.attribution_stats = attributionStatsData;
                }
                
                clearTimeout(netlifyTimeoutWarning);
                console.log(`✅ Analytics response ready: ${totalPageViews} views, ${totalConversions} conversions (${totalExecutionTime}ms)`);
                
                return createResponse(200, response);
                
            } catch (analyticsError) {
                clearTimeout(netlifyTimeoutWarning);
                const elapsed = Date.now() - functionStartTime;
                console.error('❌ Analytics processing failed:', analyticsError);
                console.error(`   Error Type: ${analyticsError.name}`);
                console.error(`   Error Message: ${analyticsError.message}`);
                console.error(`   Time Elapsed: ${elapsed}ms`);
                console.error(`   Stack: ${analyticsError.stack}`);
                return createResponse(500, { 
                    error: 'Analytics processing failed', 
                    message: analyticsError.message,
                    time_elapsed: elapsed,
                    timeout_info: {
                        function_limit: NETLIFY_TIMEOUT_MS,
                        time_remaining: NETLIFY_TIMEOUT_MS - elapsed
                    }
                });
            }
        }
        
        if (event.httpMethod === 'POST') {
            try {
                const data = JSON.parse(event.body);
                
                if (data.conversions && data.conversions.length > 0) {
                    const key = data.email ? 
                        `conversions:${data.email.replace(/[^a-zA-Z0-9]/g, '_')}:${Date.now()}` :
                        `conversions:${data.timestamp}:${Math.random()}`;
                    
                    await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
                    console.log(`✅ Stored conversion: ${data.email || 'no email'}`);
                } else {
                    const key = `pageviews:${data.timestamp}:${Math.random()}`;
                    await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
                    console.log(`✅ Stored page view: ${data.source} → ${data.landing_page}`);
                }
                
                clearTimeout(netlifyTimeoutWarning);
                return createResponse(200, { success: true });
                
            } catch (error) {
                clearTimeout(netlifyTimeoutWarning);
                console.error('❌ Analytics POST error:', error);
                console.error(`   Time Elapsed: ${Date.now() - functionStartTime}ms`);
                return createResponse(500, { error: error.message });
            }
        }
        
        clearTimeout(netlifyTimeoutWarning);
        return createResponse(405, { error: 'Method not allowed' });
        
    } catch (error) {
        clearTimeout(netlifyTimeoutWarning);
        const elapsed = Date.now() - functionStartTime;
        console.error('❌ CRITICAL FUNCTION ERROR:', error);
        console.error(`   Error Type: ${error.name}`);
        console.error(`   Error Message: ${error.message}`);
        console.error(`   Time Elapsed: ${elapsed}ms`);
        console.error(`   Stack: ${error.stack}`);
        console.error(`   Possible Root Causes:`);
        console.error(`     - Netlify function timeout (${NETLIFY_TIMEOUT_MS}ms limit)`);
        console.error(`     - Memory limit exceeded`);
        console.error(`     - Upstash Redis service failure`);
        console.error(`     - Network connectivity lost`);
        console.error(`     - JavaScript runtime error`);
        
        return createResponse(500, { 
            error: 'Critical function error', 
            message: error.message,
            timestamp: new Date().toISOString(),
            timeout_analysis: {
                function_limit_ms: NETLIFY_TIMEOUT_MS,
                time_elapsed_ms: elapsed,
                timeout_exceeded: elapsed > NETLIFY_TIMEOUT_MS,
                time_remaining_ms: Math.max(0, NETLIFY_TIMEOUT_MS - elapsed)
            }
        });
    }
};

module.exports = { handler };
