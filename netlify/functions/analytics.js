// UPDATED ANALYTICS.JS - FIXED PAGE VIEW COUNTING
// Replace the timestamp validation and filtering functions with these corrected versions

// CORRECTED: More lenient timestamp validation
function isValidTimestamp(timestamp) {
    if (!timestamp) return false;
    
    try {
        const date = new Date(timestamp);
        
        // Check if the date is valid and not NaN
        if (isNaN(date.getTime())) return false;
        
        // Check if the timestamp is reasonable - MUCH MORE LENIENT RANGE
        const timestampMs = date.getTime();
        
        // Allow timestamps from 2015 to 2035 (much wider range)
        const minDate = new Date('2015-01-01').getTime();
        const maxDate = new Date('2035-12-31').getTime();
        
        return timestampMs >= minDate && timestampMs <= maxDate;
        
    } catch (error) {
        console.warn('Timestamp validation error:', error);
        return false;
    }
}

// CORRECTED: Less aggressive timestamp processing
function safeProcessTimestamp(timestamp, fallbackTimestamp = null) {
    // FIRST: Try to use timestamp as-is if it's valid
    if (isValidTimestamp(timestamp)) {
        return timestamp;
    }
    
    // SECOND: Try common timestamp fixes
    if (timestamp) {
        // Handle Unix timestamps (convert to ISO)
        if (typeof timestamp === 'number' || /^\d{10,13}$/.test(timestamp)) {
            const unixTimestamp = new Date(parseInt(timestamp) * (timestamp.length === 10 ? 1000 : 1));
            if (isValidTimestamp(unixTimestamp.toISOString())) {
                console.log('🔧 Converted Unix timestamp:', timestamp, '→', unixTimestamp.toISOString());
                return unixTimestamp.toISOString();
            }
        }
        
        // Handle other timestamp formats
        try {
            const fixedDate = new Date(timestamp);
            if (!isNaN(fixedDate.getTime())) {
                const isoString = fixedDate.toISOString();
                if (isValidTimestamp(isoString)) {
                    console.log('🔧 Fixed timestamp format:', timestamp, '→', isoString);
                    return isoString;
                }
            }
        } catch (e) {
            // Continue to fallback
        }
    }
    
    console.warn('⚠️ Could not fix timestamp, using fallback:', timestamp);
    
    // THIRD: Use fallback if provided and valid
    if (fallbackTimestamp && isValidTimestamp(fallbackTimestamp)) {
        console.log('✅ Using fallback timestamp:', fallbackTimestamp);
        return fallbackTimestamp;
    }
    
    // LAST RESORT: Generate current timestamp
    const currentTimestamp = new Date().toISOString();
    console.log('🔧 Generated current timestamp fallback:', currentTimestamp);
    return currentTimestamp;
}

// CORRECTED: More lenient filtering
function applyFilters(data, filters) {
    let filtered = data;
    
    console.log(`🔍 Applying filters to ${data.length} items:`, filters);
    
    if (filters.start_date) {
        const startDate = new Date(filters.start_date);
        const beforeFilter = filtered.length;
        
        filtered = filtered.filter(item => {
            // Use the safe timestamp processing instead of direct validation
            const safeTimestamp = safeProcessTimestamp(item.timestamp);
            try {
                const itemDate = new Date(safeTimestamp);
                return itemDate >= startDate;
            } catch (e) {
                console.warn('⚠️ Date filter error for item:', item.timestamp, e);
                return true; // INCLUDE items with problematic timestamps instead of excluding
            }
        });
        
        console.log(`📅 Start date filter (${filters.start_date}): ${beforeFilter} → ${filtered.length} (removed ${beforeFilter - filtered.length})`);
    }
    
    if (filters.end_date) {
        const endDate = new Date(filters.end_date);
        endDate.setHours(23, 59, 59, 999); // Include entire end date
        const beforeFilter = filtered.length;
        
        filtered = filtered.filter(item => {
            // Use the safe timestamp processing instead of direct validation
            const safeTimestamp = safeProcessTimestamp(item.timestamp);
            try {
                const itemDate = new Date(safeTimestamp);
                return itemDate <= endDate;
            } catch (e) {
                console.warn('⚠️ Date filter error for item:', item.timestamp, e);
                return true; // INCLUDE items with problematic timestamps instead of excluding
            }
        });
        
        console.log(`📅 End date filter (${filters.end_date}): ${beforeFilter} → ${filtered.length} (removed ${beforeFilter - filtered.length})`);
    }
    
    if (filters.source) {
        const beforeFilter = filtered.length;
        filtered = filtered.filter(item => item.source === filters.source);
        console.log(`🎯 Source filter (${filters.source}): ${beforeFilter} → ${filtered.length} (removed ${beforeFilter - filtered.length})`);
    }
    
    if (filters.campaign) {
        const beforeFilter = filtered.length;
        filtered = filtered.filter(item => 
            (item.utm_campaign || item.campaign) === filters.campaign
        );
        console.log(`📢 Campaign filter (${filters.campaign}): ${beforeFilter} → ${filtered.length} (removed ${beforeFilter - filtered.length})`);
    }
    
    return filtered;
}

// CORRECTED: Attribution data processing with detailed logging
if (attributionKeys.length > 0) {
    try {
        console.log('📦 Fetching attribution data...');
        
        if (attributionKeys.length > 5000) {
            console.log(`⚠️ Large dataset: ${attributionKeys.length} keys. Processing in batches...`);
            
            const batchSize = 1000;
            for (let i = 0; i < attributionKeys.length; i += batchSize) {
                const batch = attributionKeys.slice(i, i + batchSize);
                const batchData = await redis(`mget/${batch.join('/')}`);
                
                if (batchData.result) {
                    const parsedBatch = batchData.result
                        .filter(item => item)
                        .map(item => {
                            try {
                                const parsed = JSON.parse(decodeURIComponent(item));
                                
                                // CORRECTED: Don't modify timestamp unless absolutely necessary
                                const originalTimestamp = parsed.timestamp;
                                parsed.timestamp = safeProcessTimestamp(parsed.timestamp);
                                
                                if (parsed.timestamp !== originalTimestamp) {
                                    console.log(`🔧 Batch ${Math.floor(i/batchSize) + 1}: Fixed timestamp ${originalTimestamp} → ${parsed.timestamp}`);
                                }
                                
                                return parsed;
                            } catch (parseError) {
                                console.warn('⚠️ Failed to parse attribution item in batch:', parseError);
                                return null;
                            }
                        })
                        .filter(item => item !== null);
                    
                    allPageViews = allPageViews.concat(parsedBatch);
                    console.log(`✅ Batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(attributionKeys.length/batchSize)}: processed ${parsedBatch.length} records`);
                }
            }
            
            console.log(`📊 Successfully parsed ${allPageViews.length} page views from batches`);
        } else {
            // Normal processing for smaller datasets
            const attributionData = await redis(`mget/${attributionKeys.join('/')}`);
            const rawPageViews = attributionData.result || [];
            console.log(`📊 Retrieved ${rawPageViews.length} raw page view records`);
            
            allPageViews = rawPageViews
                .filter(item => item)
                .map(item => {
                    try {
                        const parsed = JSON.parse(decodeURIComponent(item));
                        
                        // CORRECTED: Don't modify timestamp unless absolutely necessary
                        const originalTimestamp = parsed.timestamp;
                        parsed.timestamp = safeProcessTimestamp(parsed.timestamp);
                        
                        if (parsed.timestamp !== originalTimestamp) {
                            console.log(`🔧 Fixed timestamp ${originalTimestamp} → ${parsed.timestamp}`);
                        }
                        
                        return parsed;
                    } catch (parseError) {
                        console.warn('⚠️ Failed to parse attribution item:', parseError);
                        return null;
                    }
                })
                .filter(item => item !== null);
                
            console.log(`📊 Successfully parsed ${allPageViews.length} page views`);
        }
        
        // IPv6/IPv4 metrics for debugging
        const ipv4Count = allPageViews.filter(pv => pv.ip_address && !pv.ip_address.includes(':')).length;
        const ipv6Count = allPageViews.filter(pv => pv.ip_address && pv.ip_address.includes(':')).length;
        console.log(`📊 Page view data - IPv4: ${ipv4Count}, IPv6: ${ipv6Count}`);
        
        if (ipv6Count > 0) {
            const sampleIpv6 = allPageViews
                .filter(pv => pv.ip_address && pv.ip_address.includes(':'))
                .slice(0, 3)
                .map(pv => pv.ip_address);
            console.log('🌐 Sample IPv6 IPs in data:', sampleIpv6);
        }
        
    } catch (attributionError) {
        console.error('❌ Attribution data fetch error:', attributionError);
        allPageViews = [];
    }
}

// CORRECTED: Protected daily trends calculation
const protectedDailyTrends = {};

filteredPageViews.forEach(item => {
    try {
        // Use the corrected safe timestamp processing
        const safeTimestamp = safeProcessTimestamp(item.timestamp);
        const date = new Date(safeTimestamp).toISOString().split('T')[0];
        
        if (!protectedDailyTrends[date]) {
            protectedDailyTrends[date] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set() };
        }
        protectedDailyTrends[date].pageViews++;
        if (item.ip_address) {
            protectedDailyTrends[date].uniqueVisitors.add(item.ip_address);
        }
        
    } catch (dateError) {
        // CORRECTED: Don't skip items, just log the warning
        console.warn('⚠️ Daily trends: problematic timestamp (included anyway):', item.timestamp);
        
        // Include in "unknown" date bucket instead of skipping
        const unknownDate = 'unknown-date';
        if (!protectedDailyTrends[unknownDate]) {
            protectedDailyTrends[unknownDate] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set() };
        }
        protectedDailyTrends[unknownDate].pageViews++;
        if (item.ip_address) {
            protectedDailyTrends[unknownDate].uniqueVisitors.add(item.ip_address);
        }
    }
});

filteredConversions.forEach(conversion => {
    try {
        const safeTimestamp = safeProcessTimestamp(conversion.timestamp);
        const date = new Date(safeTimestamp).toISOString().split('T')[0];
        
        if (!protectedDailyTrends[date]) {
            protectedDailyTrends[date] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set() };
        }
        protectedDailyTrends[date].conversions++;
        
    } catch (dateError) {
        console.warn('⚠️ Daily trends: problematic conversion timestamp (included anyway):', conversion.timestamp);
        
        // Include in "unknown" date bucket instead of skipping
        const unknownDate = 'unknown-date';
        if (!protectedDailyTrends[unknownDate]) {
            protectedDailyTrends[unknownDate] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set() };
        }
        protectedDailyTrends[unknownDate].conversions++;
    }
});

// Use protectedDailyTrends instead of dailyTrends
const dailyTrends = Object.entries(protectedDailyTrends)
    .filter(([date]) => date !== 'unknown-date') // Exclude unknown dates from trends
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, data]) => ({
        date,
        pageViews: data.pageViews,
        conversions: data.conversions,
        uniqueVisitors: data.uniqueVisitors.size,
        conversionRate: data.uniqueVisitors.size > 0 ?
            (data.conversions / data.uniqueVisitors.size * 100).toFixed(1) : '0.0'
    }));

// ADDED: Enhanced debug information
console.log(`📊 FINAL DATA SUMMARY:`);
console.log(`  - Raw page views processed: ${allPageViews.length}`);
console.log(`  - After filters applied: ${filteredPageViews.length}`);
console.log(`  - Raw conversions processed: ${allConversions.length}`);
console.log(`  - After filters applied: ${filteredConversions.length}`);
console.log(`  - Unique visitors calculated: ${uniqueVisitors}`);
console.log(`  - Daily trends calculated: ${dailyTrends.length} days`);

// Build response object with enhanced debug info
const response = {
    summary: {
        total_page_views: totalPageViews,
        unique_visitors: uniqueVisitors,
        total_conversions: totalConversions,
        // ... rest of summary
    },
    // ... rest of response
    
    debug: {
        attribution_keys_found: attributionKeys.length,
        conversion_keys_found: conversionKeys.length,
        raw_page_views_processed: allPageViews.length,
        filtered_page_views: filteredPageViews.length,
        page_views_lost_to_filtering: allPageViews.length - filteredPageViews.length,
        raw_conversions_processed: allConversions.length,
        filtered_conversions: filteredConversions.length,
        conversions_lost_to_filtering: allConversions.length - filteredConversions.length,
        timestamp_corrections_applied: true,
        deployment_timestamp: new Date().toISOString(),
        redis_method: 'corrected_lenient_timestamp_approach'
    }
};

console.log('🔧 Enhanced analytics.js corrections applied!');
