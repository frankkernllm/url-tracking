// Test Recovery Script - Process first few webhooks to verify system
const fs = require('fs').promises;
const Papa = require('papaparse');

class TestRecoveryProcessor {
  constructor() {
    this.recoveryEndpoint = 'https://trackingojoy.netlify.app/.netlify/functions/dual-ip-recovery';
  }

  // Extract dual IP data from webhook payload - CORRECTED per technical brief
  extractDualIPs(webhookData) {
    const data = webhookData.data;
    
    // CORRECTED: Pageview IP is top-level (where attribution is stored)
    const pageviewIP = data.ip;
    
    // CORRECTED: Conversion IP is nested (always IPv4, for reference)
    const conversionIP = data.checkoutview?.pageviewcheckout?.pageview?.ip;
    
    return {
      pageviewIP,
      conversionIP,
      hasDualIP: !!(pageviewIP && conversionIP),
      ipsDifferent: !!(pageviewIP && conversionIP && pageviewIP !== conversionIP)
    };
  }

  // Format webhook data for recovery
  formatForRecovery(webhookRow, webhookData) {
    const data = webhookData.data;
    const ipData = this.extractDualIPs(webhookData);
    
    return {
      email: data.email,
      order_id: data.order_id,
      order_total: data.order_total,
      ip: ipData.pageviewIP,              // Pageview IP (where attribution stored)
      pageview_ip: ipData.pageviewIP,     // Explicit pageview IP
      conversion_ip: ipData.conversionIP, // Conversion IP (reference)
      timestamp: data.created_at || webhookRow.created_at,
      customer_id: data.customer_id,
      webhook_id: webhookRow.id,
      has_dual_ip: ipData.hasDualIP,
      ips_different: ipData.ipsDifferent
    };
  }

  // Test with just a few promising records
  async testRecovery(csvFilePath, testCount = 5) {
    console.log('🧪 Testing Recovery Process...\n');
    
    // Read and parse CSV
    const csvContent = await fs.readFile(csvFilePath, 'utf8');
    const parsed = Papa.parse(csvContent, {
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true
    });

    console.log(`📊 Found ${parsed.data.length} total webhooks`);
    console.log(`🎯 Testing with ${testCount} records that have different IPs\n`);

    // Find webhooks with different IPs (most likely to need recovery)
    const testCandidates = [];
    
    for (const row of parsed.data) {
      try {
        const webhookData = JSON.parse(row.request);
        const conversionData = this.formatForRecovery(row, webhookData);
        
        if (conversionData.ips_different) {
          testCandidates.push({ row, webhookData, conversionData });
        }
        
        if (testCandidates.length >= testCount) break;
        
      } catch (error) {
        console.log(`⚠️  Skipping webhook ${row.id}: ${error.message}`);
      }
    }

    if (testCandidates.length === 0) {
      console.log('❌ No webhooks found with different IPs. Testing with first few records...');
      
      // Fallback to first few records
      for (let i = 0; i < Math.min(testCount, parsed.data.length); i++) {
        const row = parsed.data[i];
        try {
          const webhookData = JSON.parse(row.request);
          const conversionData = this.formatForRecovery(row, webhookData);
          testCandidates.push({ row, webhookData, conversionData });
        } catch (error) {
          console.log(`⚠️  Skipping webhook ${row.id}: ${error.message}`);
        }
      }
    }

    console.log(`\n🔍 Testing ${testCandidates.length} webhook(s):\n`);

    // Test each candidate
    for (let i = 0; i < testCandidates.length; i++) {
      const { conversionData } = testCandidates[i];
      
      console.log(`--- Test ${i + 1}/${testCandidates.length} ---`);
      console.log(`📧 Email: ${conversionData.email}`);
      console.log(`🔢 Order: ${conversionData.order_id}`);
      console.log(`👁️  Pageview IP: ${conversionData.pageview_ip || 'none'}`);
      console.log(`🛒 Conversion IP: ${conversionData.conversion_ip || 'none'}`);
      console.log(`🎯 Both IPs Available: ${conversionData.pageview_ip && conversionData.conversion_ip ? 'YES' : 'NO'}`);
      console.log(`✨ Has Dual IP: ${conversionData.has_dual_ip ? 'YES' : 'NO'}`);
      console.log(`🔄 Different IPs: ${conversionData.ips_different ? 'YES' : 'NO'}`);
      
      // Show what would be sent to recovery endpoint
      console.log(`\n📤 Recovery payload:`);
      console.log(JSON.stringify(conversionData, null, 2));
      
      // Ask user if they want to send this test
      const readline = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
      });
      
      const answer = await new Promise(resolve => {
        readline.question('\n🤔 Send this test to recovery endpoint? (y/n): ', resolve);
      });
      
      if (answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes') {
        try {
          console.log('📡 Sending to recovery endpoint...');
          
          const response = await fetch(this.recoveryEndpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(conversionData)
          });

          const result = await response.json();
          
          if (response.ok) {
            console.log('✅ Recovery test successful!');
            console.log('📋 Result:', JSON.stringify(result, null, 2));
          } else {
            console.log('❌ Recovery test failed');
            console.log('📋 Error:', JSON.stringify(result, null, 2));
          }
          
        } catch (error) {
          console.log('❌ Network error:', error.message);
        }
      } else {
        console.log('⏭️  Skipping this test');
      }
      
      readline.close();
      console.log('\n' + '='.repeat(50) + '\n');
    }

    console.log('🧪 Test complete!');
    console.log('\nIf tests look good, run the full recovery with:');
    console.log('node historical-recovery.js kernwebhookrequests20250701\\ 14_01_58.csv');
  }
}

// Main execution
async function main() {
  const csvFilePath = process.argv[2] || 'kernwebhookrequests20250701 14_01_58.csv';
  const testCount = parseInt(process.argv[3]) || 3;
  
  console.log('🧪 Historical Recovery Test Script');
  console.log('==================================\n');
  console.log(`Testing with: ${csvFilePath}`);
  console.log(`Test count: ${testCount}\n`);

  try {
    const processor = new TestRecoveryProcessor();
    await processor.testRecovery(csvFilePath, testCount);
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = TestRecoveryProcessor;
