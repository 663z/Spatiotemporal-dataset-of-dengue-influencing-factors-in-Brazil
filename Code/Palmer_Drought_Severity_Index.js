// defining the folder for saving data 
var folder = 'PDSI';



// Step 1:  Preparing study sites   

  // 5 zones, 560 microregions
  // users/663zhuqx/MicroCentralWest
  // users/663zhuqx/MicroNorth
  // users/663zhuqx/MicroNorthEast 
  // users/663zhuqx/MicroSouth
  // users/663zhuqx/MicroSouthEast
var aois = ee.FeatureCollection("users/663zhuqx/MicroNorth");
Map.addLayer(aois);
var aois= aois.toList(aois.size());
print('aois',aois);



// Step 2:  Preparing months 
var startDate = ee.Date('2001-1-1'); // first day in first week  of 2001 
var endDate = ee.Date('2025-1-1'); // last day in last week of 2024
var nMonths = endDate.difference(startDate, 'month');
var monthOffsets = ee.List.sequence(0, nMonths.subtract(1)); 
print('months', monthOffsets);


//  Step 3:  Setting the weighting variable   
var pop_col = ee.ImageCollection([]);
for (var year = 2000; year <= 2020; year++) {
    var popWeight_img = ee.Image('WorldPop/GP/100m/pop/BRA_' + year)
        .set('year', year);
    pop_col = pop_col.merge(ee.ImageCollection([popWeight_img]));
}
// Using 2020 population data from 2001-2024
for (var year = 2021; year <= 2024; year++) {
    var popWeight_img = ee.Image('WorldPop/GP/100m/pop/BRA_2020')
        .set('year', year);
    pop_col = pop_col.merge(ee.ImageCollection([popWeight_img]));
}



// Step 4: Computing the variables  
for (var i = 0 ; i < aois.size().getInfo(); i++){


var aoi = ee.Feature(aois.get(i));
var label = aoi.get('CD_MICRO').getInfo();  // CD_MICRO column name in the attribute table 
print('Processing microregion:', label, 'Progress:', (i + 1) + '/' + aois.size().getInfo()); 



var pdsi = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE').filterDate(startDate, endDate.advance(1,'month')).select('pdsi');
var pdsi_rescale = pdsi.map(function(img) {
  return img.multiply(0.01)   
            .rename('pdsi')
            .set('system:time_start',img.get('system:time_start'));
});





var pdsi_monthly = ee.ImageCollection.fromImages(
  monthOffsets.map(function(mOffset) {
    var start = startDate.advance(mOffset, 'month');
    var end = start.advance(1, 'month');
    var year = start.get('year');
    var filteredImgs = pdsi_rescale.filterDate(start, end);
    var composite = ee.Image(ee.Algorithms.If(filteredImgs.size().gt(0),
                                               filteredImgs.sum(),
                                               ee.Image(-999).rename('pdsi')));
    var effectiveYear = ee.Number(year).min(2020);
    var popWeight = pop_col.filter(ee.Filter.eq('year', effectiveYear)).first();
    
    composite = composite.addBands(popWeight);
    
    return composite
             .set('year', year)
             .set('month', start.format('YYYY-MM'))
             .set('date', start.format('YYYY-MM-dd'));
  })
);



// exporting and population-based weighting
var fc_pdsi = pdsi_monthly.map(function(img){
  var dic = img.reduceRegion({
    reducer: ee.Reducer.mean().splitWeights(),
    scale: 4000,
    geometry: aoi.geometry(),
    maxPixels: 1e13,
    tileScale:4
  });
  return ee.Feature(null,dic).set('month',img.get('month'));
}).filter(ee.Filter.notNull(['mean']));


Export.table.toDrive(fc_pdsi, 'pdsi_'+label, folder, null, 'CSV', ['month','mean']);

}