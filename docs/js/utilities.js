var dominantRiskCache = new Map();
var uploadedPDFs = [];
var enhancedRiskCache = new Map();

function normalizeText(text) {
    return text.toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-z0-9_]/g, '')
        .trim();
}

function getDominantRiskType(properties) {
    var damRisk = parseFloat(properties['dam_risk_score']) || 0;
    var fireRisk = parseFloat(properties['fire_risk_score']) || 0;
    var naturalRisk = parseFloat(properties['cemaden_risk_score']) || 0;
    
    if (damRisk <= 0.1 && fireRisk <= 0.1 && naturalRisk <= 0.1) {
        return 'no_risk';
    }
    
    var maxRisk = Math.max(damRisk, fireRisk, naturalRisk);
    
    if (maxRisk === damRisk && damRisk > 0) {
        return 'dam';
    } else if (maxRisk === fireRisk && fireRisk > 0) {
        return 'fire';
    } else if (maxRisk === naturalRisk && naturalRisk > 0) {
        return 'natural';
    } else {
        return 'no_risk';
    }
}

function optimizeMunicipalBoundaries() {
    if (!allLayers.heritage || !allLayers.municipal_boundaries) return;
    
    var activeMunicipalities = new Set();
    
    // Collect municipalities with heritage sites
    allLayers.heritage.eachLayer(function(layer) {
        var mun = normalizeText(layer.feature.properties['municipality'] || '');
        var state = normalizeText(layer.feature.properties['uf'] || '');
        activeMunicipalities.add(mun + '_' + state);
    });
    
    // Hide municipalities without heritage
    var hiddenCount = 0;
    allLayers.municipal_boundaries.eachLayer(function(layer) {
        var munName = normalizeText(layer.feature.properties['NM_MUN'] || '');
        var munState = normalizeText(layer.feature.properties['SIGLAUF'] || '');
        var key = munName + '_' + munState;
        
        if (!activeMunicipalities.has(key)) {
            layer.setStyle({ opacity: 0, fillOpacity: 0 });
            layer.options.interactive = false;
            hiddenCount++;
        }
    });
    
    console.log('Performance optimization: hidden', hiddenCount, 'empty municipalities');
}

function loadLayerProgressively(layerName, callback) {
    switch(layerName) {
        case 'heritage':
            if (typeof json_comprehensive_heritage_risk_4 !== 'undefined') {
                setTimeout(callback, 100); // Small delay for UI update
            } else {
                setTimeout(function() { loadLayerProgressively(layerName, callback); }, 100);
            }
            break;
        case 'municipal_risk':
            if (typeof json_municipalities_with_combined_risk_simplified_2 !== 'undefined') {
                setTimeout(callback, 100);
            } else {
                setTimeout(function() { loadLayerProgressively(layerName, callback); }, 100);
            }
            break;
        case 'dam_buffers':
            if (typeof json_snisb_dam_buffers_3 !== 'undefined') {
                setTimeout(callback, 100);
            } else {
                setTimeout(function() { loadLayerProgressively(layerName, callback); }, 100);
            }
            break;
        case 'municipal_boundaries':
            if (typeof json_municipalities_1_simplified_1 !== 'undefined') {
                setTimeout(callback, 100);
            } else {
                setTimeout(function() { loadLayerProgressively(layerName, callback); }, 100);
            }
            break;
    }
}

function updateLoadingProgress(percent, message) {
    var indicator = document.getElementById('loadingIndicator');
    indicator.innerHTML = '<div style="text-align: center;">' +
                         '<div style="font-size: 14px; margin-bottom: 5px;">' + message + '</div>' +
                         '<div style="background: rgba(255,255,255,0.3); height: 4px; border-radius: 2px; overflow: hidden;">' +
                         '<div style="background: #3498db; height: 100%; width: ' + percent + '%; transition: width 0.3s ease;"></div>' +
                         '</div>' +
                         '<div style="font-size: 12px; margin-top: 3px;">' + percent + '%</div>' +
                         '</div>';
}

function initializeProgressiveLoading() {
    updateLoadingProgress(0, 'Iniciando carregamento...');
    
    loadLayerProgressively('heritage', function() {
        updateLoadingProgress(25, 'Carregando sítios de patrimônio...');
        
        loadLayerProgressively('municipal_risk', function() {
            updateLoadingProgress(50, 'Carregando áreas de risco...');
            
            loadLayerProgressively('dam_buffers', function() {
                updateLoadingProgress(75, 'Carregando zonas de barragens...');
                
                loadLayerProgressively('municipal_boundaries', function() {
                    updateLoadingProgress(90, 'Otimizando limites municipais...');
                    
                    setTimeout(function() {
                        checkDataStatus();
                        optimizeMunicipalBoundaries();
                        updateLoadingProgress(100, 'Concluído!');
                        
                        setTimeout(function() {
                            document.getElementById('loadingIndicator').style.display = 'none';
                            applyAllFilters();
                        }, 500);
                    }, 300);
                });
            });
        });
    });
}

function getICMRiskFactor(feature) {
    var municipality = normalizeText(feature.properties['municipality'] || '');
    var uf = normalizeText(feature.properties['uf'] || '');
    var municipalityKey = municipality + '_' + uf;
    var icmClass = window.icmData && window.icmData[municipalityKey] ? window.icmData[municipalityKey] : null;
    
    // ICM risk multipliers - worse classification = higher risk
    switch(icmClass) {
        case 'A': return 0.1;  // Best infrastructure - lowest risk
        case 'B': return 0.3;  // Good infrastructure - low risk
        case 'C': return 0.6;  // Poor infrastructure - medium risk
        case 'D': return 1.0;  // Worst infrastructure - highest risk
        default: return 0.5;   // Unknown - medium risk
    }
}

// Calculate PDF availability factor
function getPDFAvailabilityFactor(feature) {
    var municipality = normalizeText(feature.properties['municipality'] || '');
    var uf = normalizeText(feature.properties['uf'] || '');
    var riskScore = parseFloat(feature.properties['comprehensive_risk_score']) || 0;
    
    // Check if site has uploaded PDFs with emergency protocols
    var hasEmergencyPDFs = uploadedPDFs.some(function(pdf) {
        var name = pdf.name.toLowerCase();
        return (name.includes('emergency') || name.includes('emergencia') || 
                name.includes('protocol') || name.includes('protocolo') ||
                name.includes(municipality) || name.includes(uf));
    });
    
    // Sites with risk but no emergency documentation get penalty
    if (riskScore > 0 && !hasEmergencyPDFs) {
        return 0.2;  // Documentation gap increases risk
    }
    
    return 0.0;  // No penalty if docs available or no risk
}

// Calculate enhanced comprehensive risk score
function calculateEnhancedRisk(feature) {
    var featureId = feature.properties['identificacao_bem'] || feature.properties['id'] || Math.random();
    
    // Check cache first
    if (enhancedRiskCache.has(featureId)) {
        return enhancedRiskCache.get(featureId);
    }
    
    // Base risk components
    var damRisk = parseFloat(feature.properties['dam_risk_score']) || 0;
    var fireRisk = parseFloat(feature.properties['fire_risk_score']) || 0;
    var naturalRisk = parseFloat(feature.properties['cemaden_risk_score']) || 0;
    
    // Base risk calculation - weighted combination of all risk types
    var baseRisk = (damRisk * 0.4) + (fireRisk * 0.35) + (naturalRisk * 0.25);
    
    // ICM Classification Factor (municipal capacity)
    var icmFactor = getICMRiskFactor(feature);
    
    // PDF Availability Factor (emergency preparedness)
    var pdfFactor = getPDFAvailabilityFactor(feature);
    
    // Enhanced Risk Formula:
    // Final Risk = (Base Risk * 0.6) + (ICM Factor * 0.25) + (PDF Factor * 0.15)
    var enhancedRisk = (baseRisk * 0.6) + (icmFactor * 0.25) + (pdfFactor * 0.15);
    
    // Ensure risk is within reasonable bounds
    enhancedRisk = Math.max(0, Math.min(enhancedRisk, 3.0));
    
    // Cache the result
    enhancedRiskCache.set(featureId, enhancedRisk);
    
    return enhancedRisk;
}

// Get enhanced risk level label
function getEnhancedRiskLevel(enhancedRisk) {
    if (enhancedRisk <= 0.1) return { level: 'Sem Risco', color: '#95a5a6' };
    if (enhancedRisk <= 0.72) return { level: 'Risco Médio', color: '#f1c40f' };
    if (enhancedRisk <= 1.5) return { level: 'Risco Alto', color: '#e67e22' };
    return { level: 'Risco Muito Alto', color: '#e74c3c' };
}

// Clear risk cache when PDFs change
function clearRiskCache() {
    enhancedRiskCache.clear();
    dominantRiskCache.clear();
}
