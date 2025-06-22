// Add this function to your batch processing script (the one with stricter criteria)
// This function was referenced but missing

function compareGeographicDataStrict(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    let score = 0;
    if (cityMatch) score += 3;
    if (regionMatch) score += 2;
    if (countryMatch) score += 1;
    if (ispMatch) score += 2;

    let confidence = 'NO_MATCH';
    let isMatch = false;

    // STRICT: Requires city match AND additional criteria (score >= 5) instead of >= 4
    if (score >= 6) {
        confidence = 'DEFINITE';
        isMatch = true;
    } else if (score >= 5) {
        confidence = 'STRONG';
        isMatch = true;
    }
    // No POSSIBLE category in strict mode - city match required

    return {
        isMatch,
        confidence,
        score,
        cityMatch,
        regionMatch, 
        countryMatch,
        ispMatch
    };
}
