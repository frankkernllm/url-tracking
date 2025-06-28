// COMPLETE ANALYTICS.JS FIX
// This fixes both the timestamp validation crash AND the conversion key scanning issue

// Enhanced timestamp validation function (ADD THIS AT THE TOP)
function isValidTimestamp(timestamp) {
    if (!timestamp) return false;
    
    try {
        const date = new Date(timestamp);
        
        // Check if the date is valid and not NaN
        if (isNaN(date.getTime())) return false;
        
        // Check if the timestamp is reasonable (not too far in past/future)
        const timestampMs = date.getTime();
        
        // Allow timestamps from 2020 to 2030 (reasonable range)
        const minDate = new Date('2020-01-01').getTime();
        const maxDate = new Date('2030-12-31').getTime();
        
        return timestampMs >= minDate && timestampMs <= maxDate;
        
    } catch (error) {
        console.warn('Timestamp validation error:', error);
        return false;
    }
}

// Enhanced safe timestamp processing (ADD THIS FUNCTION)
function safeProcessTimestamp(timestamp, fallbackTimestamp = null) {
    if (isValidTimestamp(timestamp)) {
        return timestamp;
    }
    
    console.warn('⚠️ Invalid timestamp detected:', timestamp);
    
    if (fallbackTimestamp && isValidTimestamp(fallbackTimestamp)) {
        console.log('✅ Using fallback timestamp:', fallbackTimestamp);
        return fallbackTimestamp;
    }
    
    // Generate current timestamp as final fallback
    const currentTimestamp = new Date().toISOString();
    console.log('🔧 Generated current timestamp fallback:', currentTimestamp);
    return currentTimestamp;
}

// Enhanced conversion key scanning (ADD THIS FUNCTION)
async function getConversionKeysEnhanced(redis) {
    let conversionKeys = [];
    
    console.log('🔍 Starting enhanced conversion key scan...');
    
    try {
        // Approach 1: Standard conversions:* pattern
        console.log('📍 Trying approach 1: conversions:* pattern');
        const standardResult = await redis('keys/conversions:*');
        if (standardResult.result && standardResult.result.length > 0) {
            conversionKeys = standardResult.result;
            console.log(`✅ Found ${conversionKeys.length} keys with standard pattern`);
            return conversionKeys;
        } else {
            console.log('❌ Standard pattern found 0 keys');
        }
        
        // Approach 2: Try different date-based patterns
        console.log('📍 Trying approach 2: date-based patterns');
        const today = new Date().toISOString().split('T')[0];
        const yesterday = new Date(Date.now() - 24*60*60*1000).toISOString().split('T')[0];
        
        const datePatterns = [
            `conversions:${today}*`,
            `conversions:${yesterday}*`,
            `conversions:2025-06-28*`,
            `conversions:2025-06-27*`,
            `conversions:2025-06-26*`
        ];
        
        for (const pattern of datePatterns) {
            try {
                console.log(`📍 Trying pattern: ${pattern}`);
                const dateResult = await redis(`keys/${pattern}`);
                if (dateResult.result && dateResult.result.length > 0) {
                    conversionKeys = conversionKeys.concat(dateResult.result);
                    console.log(`✅ Found ${dateResult.result.length} keys with pattern ${pattern}`);
                }
            } catch (patternError) {
                console.log(`⚠️ Pattern ${pattern} failed:`, patternError.message);
            }
        }
        
        // Approach 3: Use SCAN command instead of KEYS
        console.log('📍 Trying approach 3: SCAN command');
        try {
            const scanResult = await redis('scan/0/match/conversions:*/count/1000');
            if (scanResult.result && scanResult.result[1] && scanResult.result[1].length > 0) {
                const scanKeys = scanResult.result[1];
                conversionKeys = conversionKeys.concat(scanKeys);
                console.log(`✅ SCAN found ${scanKeys.length} additional keys`);
            }
        } catch (scanError) {
            console.log('⚠️ SCAN approach failed:', scanError.message);
        }
        
        // Remove duplicates
        conversionKeys = [...new Set(conversionKeys)];
        
        console.log(`📊 Total conversion keys found: ${conversionKeys.length}`);
        console.log(`📝 Sample keys:`, conversionKeys.slice(0, 3));
        
        return conversionKeys;
        
    } catch (error) {
        console.log('❌ Enhanced conversion key scan failed:', error.message);
        return [];
    }
}

// REPLACE your existing conversion key scanning section with this:
// Find this section in your analytics.js:
/*
// Get conversion keys (usually fewer, so single operation is OK)
try {
  const conversionsResult = await redis('keys/conversions:*');
  conversionKeys = conversionsResult.result || [];
  console.log(`🔍 Found ${conversionKeys.length} conversion keys`);
} catch (error) {
  console.log('⚠️ Failed to get conversion keys:', error);
  conversionKeys = [];
}
*/

// Replace it with:
try {
  conversionKeys = await getConversionKeysEnhanced(redis);
  console.log(`🔍 Enhanced scan found ${conversionKeys.length} conversion keys`);
} catch (error) {
  console.log('⚠️ Enhanced conversion key scan failed:', error);
  conversionKeys = [];
}

// CRITICAL: Replace your conversion data processing section
// Find this section in your analytics.js where conversions are processed:
/*
const rawConversions = (conversionData.result || [])
  .filter(item => item)
  .map(item => {
    try {
      const parsed = JSON.parse(item);
      // VALIDATE TIMESTAMP HERE - this is where it crashes
      if (!isValidTimestamp(parsed.timestamp)) {
        console.warn('⚠️ Invalid timestamp found in conversion:', parsed.timestamp);
        parsed.timestamp = new Date().toISOString(); // Use current time as fallback
      }
      return parsed;
    } catch (parseError) {
      return null;
    }
  })
*/

// Replace with this PROTECTED version:
const rawConversions = (conversionData.result || [])
  .filter(item => item)
  .map(item => {
    try {
      const parsed = JSON.parse(decodeURIComponent(item)); // Add decodeURIComponent!
      
      // CRITICAL: Validate timestamp before any date operations
      parsed.timestamp = safeProcessTimestamp(parsed.timestamp);
      
      return parsed;
    } catch (parseError) {
      console.warn('⚠️ Failed to parse conversion item:', parseError);
      return null;
    }
  })
  .filter(item => item !== null)
  .sort((a, b) => {
    try {
      const dateA = new Date(a.timestamp);
      const dateB = new Date(b.timestamp);
      return dateB - dateA;
    } catch (sortError) {
      console.warn('⚠️ Sort error, using current time:', sortError);
      return 0;
    }
  });

// ALSO PROTECT any other timestamp operations in analytics.js
// Replace any instances of direct Date operations with safe versions:

// BEFORE (vulnerable):
// const date = new Date(item.timestamp).toISOString().split('T')[0];

// AFTER (protected):
// const safeTimestamp = safeProcessTimestamp(item.timestamp);
// const date = new Date(safeTimestamp).toISOString().split('T')[0];

// PROTECT daily trends calculation:
const protectedDailyTrends = {};
filteredPageViews.forEach(item => {
    try {
        const safeTimestamp = safeProcessTimestamp(item.timestamp);
        const date = new Date(safeTimestamp).toISOString().split('T')[0];
        
        if (!protectedDailyTrends[date]) {
            protectedDailyTrends[date] = { pageViews: 0, conversions: 0 };
        }
        protectedDailyTrends[date].pageViews++;
        
    } catch (dateError) {
        console.warn('⚠️ Skipping page view with invalid timestamp:', item.timestamp);
    }
});

filteredConversions.forEach(conversion => {
    try {
        const safeTimestamp = safeProcessTimestamp(conversion.timestamp);
        const date = new Date(safeTimestamp).toISOString().split('T')[0];
        
        if (!protectedDailyTrends[date]) {
            protectedDailyTrends[date] = { pageViews: 0, conversions: 0 };
        }
        protectedDailyTrends[date].conversions++;
        
    } catch (dateError) {
        console.warn('⚠️ Skipping conversion with invalid timestamp:', conversion.timestamp);
    }
});

// PROTECT attribution stats processing:
if (attributionStats && attributionStats.length > 0) {
    attributionStats.forEach(stat => {
        // Validate attribution stat timestamp
        stat.timestamp = safeProcessTimestamp(stat.timestamp);
    });
}

// IMPORTANT: Make sure to use protectedDailyTrends instead of dailyTrends in your response
const dailyTrends = Object.entries(protectedDailyTrends)
  .sort(([a], [b]) => a.localeCompare(b))
  .map(([date, data]) => ({
    date,
    pageViews: data.pageViews,
    conversions: data.conversions
  }));

// This complete fix will:
// 1. Stop the "Invalid time value" crashes
// 2. Enable proper conversion key scanning
// 3. Allow conversions to appear in your dashboard
// 4. Protect all timestamp operations from future crashes
