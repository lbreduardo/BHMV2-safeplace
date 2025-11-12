var highlightLayer;

var autolinker = new Autolinker({truncate: {length: 30, location: 'smart'}});

function removeEmptyRowsFromPopupContent(content, feature) {
  var tempDiv = document.createElement('div');
  tempDiv.innerHTML = content;
  var rows = tempDiv.querySelectorAll('tr');
  for (var i = 0; i < rows.length; i++) {
    var td = rows[i].querySelector('td.visible-with-data');
    var key = td ? td.id : '';
    if (td && td.classList.contains('visible-with-data') && feature.properties[key] == null) {
      rows[i].parentNode.removeChild(rows[i]);
    }
  }
  return tempDiv.innerHTML;
}

// Enhanced media popup class
function addClassToPopupIfMedia(content, popup) {
  var tempDiv = document.createElement('div');
  tempDiv.innerHTML = content;
  if (tempDiv.querySelector('td img')) {
    popup._contentNode.classList.add('media');
    setTimeout(function() {
      popup.update();
    }, 10);
  } else {
    popup._contentNode.classList.remove('media');
  }
}

function highlightFeature(e) {
  highlightLayer = e.target;
  highlightLayer.openPopup();
}

function pop_snisb_dam_buffers_3(feature, layer) {
  layer.on({
    mouseout: function(e) {
      if (typeof layer.closePopup == 'function') {
        layer.closePopup();
      } else {
        layer.eachLayer(function(feature){
          feature.closePopup()
        });
      }
    },
    mouseover: highlightFeature,
  });
  var popupContent = '<table>\
    <tr>\
    <td colspan="2" class="heritage-title"><strong>' + (feature.properties['dam_name'] !== null ? autolinker.link(String(feature.properties['dam_name']).replace(/'/g, '\'').toLocaleString()) : '') + '</strong></td>\
    </tr>\
    <tr>\
    <th scope="row">Estado</th>\
    <td>' + (feature.properties['state'] !== null ? autolinker.link(String(feature.properties['state']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Município</th>\
    <td>' + (feature.properties['municipality'] !== null ? autolinker.link(String(feature.properties['municipality']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Finalidade</th>\
    <td>' + (feature.properties['purpose'] !== null ? autolinker.link(String(feature.properties['purpose']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Categoria de Risco</th>\
    <td class="risk-score">' + (feature.properties['risk_category'] !== null ? autolinker.link(String(feature.properties['risk_category']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Potencial de Dano</th>\
    <td class="risk-score">' + (feature.properties['damage_potential'] !== null ? autolinker.link(String(feature.properties['damage_potential']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Risco ao Bem Cultural</th>\
    <td class="risk-score">' + (feature.properties['heritage_risk_potential'] !== null ? autolinker.link(String(feature.properties['heritage_risk_potential']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    </table>';
  var content = removeEmptyRowsFromPopupContent(popupContent, feature);
  layer.on('popupopen', function(e) {
    addClassToPopupIfMedia(content, e.popup);
  });
  layer.bindPopup(content, { maxHeight: 400 });
}

function pop_comprehensive_heritage_risk_4(feature, layer) {
  layer.on({
      click: highlightFeature,
  });

  var municipality = normalizeText(feature.properties['municipality'] || '');
  var uf = normalizeText(feature.properties['uf'] || '');
  var municipalityKey = municipality + '_' + uf;
  var icmClassification = window.icmData && window.icmData[municipalityKey] ? window.icmData[municipalityKey] : null;

  var icmBadge = '';
  if (icmClassification) {
      var icmColor = '';
      switch(icmClassification) {
          case 'A': icmColor = '#2E7D32'; break;
          case 'B': icmColor = '#1976D2'; break;
          case 'C': icmColor = '#F57F17'; break;
          case 'D': icmColor = '#C62828'; break;
          default: icmColor = '#757575';
      }
      icmBadge = '<span style="background-color: ' + icmColor + '; color: white; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: bold;">' + icmClassification + '</span>';
  } else {
      icmBadge = '<span style="background-color: #757575; color: white; padding: 2px 8px; border-radius: 12px; font-size: 12px;">N/A</span>';
  }

  // Use enhanced risk calculation instead of basic risk score
  var enhancedRisk = calculateEnhancedRisk(feature);
  var riskInfo = getEnhancedRiskLevel(enhancedRisk);

  var dominantType = getDominantRiskType(feature.properties);
  var dominantTypeLabel = 'Sem Risco';
  var dominantTypeColor = '#27ae60';

  if (dominantType === 'dam') {
      dominantTypeLabel = 'Risco de Barragens';
      dominantTypeColor = '#e77148';
  } else if (dominantType === 'fire') {
      dominantTypeLabel = 'Risco de Fogo';
      dominantTypeColor = '#e74c3c';
  } else if (dominantType === 'natural') {
      dominantTypeLabel = 'Risco Natural';
      dominantTypeColor = '#3498db';
  }

  var popupContent = '<table>\
          <tr>\
              <td colspan="2" class="heritage-title"><strong>' + (feature.properties['identificacao_bem'] !== null ? autolinker.link(String(feature.properties['identificacao_bem']).replace(/'/g, '\'').toLocaleString()) : 'Sítio de Bem Cultural') + '</strong></td>\
          </tr>\
          <tr>\
              <th scope="row">Natureza</th>\
              <td>' + (feature.properties['ds_natureza'] !== null ? autolinker.link(String(feature.properties['ds_natureza']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
          </tr>\
          <tr>\
              <th scope="row">Tipo de Proteção</th>\
              <td>' + (feature.properties['ds_tipo_protecao'] !== null ? autolinker.link(String(feature.properties['ds_tipo_protecao']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
          </tr>\
          <tr>\
              <th scope="row">Município</th>\
              <td>' + (feature.properties['municipality'] !== null ? autolinker.link(String(feature.properties['municipality']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
          </tr>\
          <tr>\
              <th scope="row">Estado</th>\
              <td>' + (feature.properties['uf'] !== null ? autolinker.link(String(feature.properties['uf']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
          </tr>\
          <tr>\
              <th scope="row">Classificação ICM</th>\
              <td>' + icmBadge + '</td>\
          </tr>\
          <tr>\
              <th scope="row">Nível de Risco</th>\
              <td class="risk-score" style="background-color: ' + riskInfo.color + ' !important; color: white; font-weight: bold;">' + riskInfo.level + '</td>\
          </tr>\
          <tr>\
              <th scope="row">Pontuação de Risco</th>\
              <td>' + enhancedRisk.toFixed(3) + '</td>\
          </tr>\
          <tr>\
              <th scope="row">Tipo de Risco Principal</th>\
              <td style="background-color: ' + dominantTypeColor + ' !important; color: white; font-weight: bold;">' + dominantTypeLabel + '</td>\
          </tr>\
      </table>';
  var content = removeEmptyRowsFromPopupContent(popupContent, feature);
  layer.on('popupopen', function(e) {
      addClassToPopupIfMedia(content, e.popup);
  });
  layer.bindPopup(content, { maxHeight: 400 });
}

function pop_municipalities_with_combined_risk_simplified_2(feature, layer) {
  layer.on({
    mouseout: function(e) {
      if (typeof layer.closePopup == 'function') {
        layer.closePopup();
      } else {
        layer.eachLayer(function(feature){
          feature.closePopup()
        });
      }
    },
    mouseover: highlightFeature,
  });

  const props = feature.properties;
  const heatClass = props['heritage_heat_class'];

  // Determine color and label based on heat class
  let heatColor = '#95a5a6';
  let heatLabel = 'Nenhum';
  var municipality = normalizeText(feature.properties['NM_MUN'] || '');
  var uf = normalizeText(feature.properties['SIGLAUF'] || '');
  var municipalityKey = municipality + '_' + uf;
  var icmClassification = window.icmData && window.icmData[municipalityKey] ? window.icmData[municipalityKey] : null;
  var riskScore = feature.properties['comprehensive_risk_score_mean'];

  var icmBadge = '';
  if (icmClassification) {
    var icmColor = '';
    switch(icmClassification) {
      case 'A': icmColor = '#2E7D32'; break;
      case 'B': icmColor = '#1976D2'; break;
      case 'C': icmColor = '#F57F17'; break;
      case 'D': icmColor = '#C62828'; break;
      default: icmColor = '#757575';
    }
    icmBadge = '<span style="background-color: ' + icmColor + '; color: white; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: bold;">' + icmClassification + '</span>';
  } else {
    icmBadge = '<span style="background-color: #757575; color: white; padding: 2px 8px; border-radius: 12px; font-size: 12px;">N/A</span>';
  }

  if (heatClass === 'low') {
    heatColor = '#f1c40f'; // Yellow
    heatLabel = 'Baixo';
  } else if (heatClass === 'high') {
    heatColor = '#e74c3c'; // Red
    heatLabel = 'Alto';
  }

  const popupContent = '<table>\
      <tr>\
        <td colspan="2" class="heritage-title"><strong>' + (props['NM_MUN'] ?? '') + '</strong></td>\
      </tr>\
      <tr>\
        <th scope="row">Estado</th>\
        <td>' + (props['SIGLA_UF'] ?? '') + '</td>\
      </tr>\
      <tr>\
        <th scope="row">Classificação ICM</th>\
        <td>' + icmBadge + '</td>\
      </tr>\
      <tr>\
        <th scope="row">Nível de Risco</th>\
        <td class="risk-score" style="background-color: ' + heatColor + ' !important;">' + heatLabel + '</td>\
      </tr>\
      <tr>\
        <th scope="row">Pontuação de Risco</th>\
        <td class="risk-score">' + (riskScore !== null ? riskScore.toFixed(3) : 'N/A') + '</td>\
      </tr>\
      <tr>\
        <th scope="row">Bens Culturais Protegidos</th>\
        <td>' + (props['NUMPOINTS'] !== null ? props['NUMPOINTS'] : 0) + '</td>\
      </tr>\
    </table>';

  const content = removeEmptyRowsFromPopupContent(popupContent, feature);
  layer.on('popupopen', function(e) {
    addClassToPopupIfMedia(content, e.popup);
  });
  layer.bindPopup(content, { maxHeight: 400 });
}

function pop_snisb_dam_buffers_3(feature, layer) {
  layer.on({
    mouseout: function(e) {
      if (typeof layer.closePopup == 'function') {
        layer.closePopup();
      } else {
        layer.eachLayer(function(feature){
          feature.closePopup()
        });
      }
    },
    mouseover: highlightFeature,
  });
  var popupContent = '<table>\
    <tr>\
    <td colspan="2" class="heritage-title"><strong>' + (feature.properties['dam_name'] !== null ? autolinker.link(String(feature.properties['dam_name']).replace(/'/g, '\'').toLocaleString()) : '') + '</strong></td>\
    </tr>\
    <tr>\
    <th scope="row">Estado</th>\
    <td>' + (feature.properties['state'] !== null ? autolinker.link(String(feature.properties['state']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Município</th>\
    <td>' + (feature.properties['municipality'] !== null ? autolinker.link(String(feature.properties['municipality']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Finalidade</th>\
    <td>' + (feature.properties['purpose'] !== null ? autolinker.link(String(feature.properties['purpose']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Categoria de Risco</th>\
    <td class="risk-score">' + (feature.properties['risk_category'] !== null ? autolinker.link(String(feature.properties['risk_category']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Potencial de Dano</th>\
    <td class="risk-score">' + (feature.properties['damage_potential'] !== null ? autolinker.link(String(feature.properties['damage_potential']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    <tr>\
    <th scope="row">Risco ao Bem Cultural</th>\
    <td class="risk-score">' + (feature.properties['heritage_risk_potential'] !== null ? autolinker.link(String(feature.properties['heritage_risk_potential']).replace(/'/g, '\'').toLocaleString()) : '') + '</td>\
    </tr>\
    </table>';
  var content = removeEmptyRowsFromPopupContent(popupContent, feature);
  layer.on('popupopen', function(e) {
    addClassToPopupIfMedia(content, e.popup);
  });
  layer.bindPopup(content, { maxHeight: 400 });
}
