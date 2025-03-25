// defining the folder for saving data 
var folder = 'PDSI_2001/2024';



// Step 1:  Preparing study sites 
  // 5 zones, 560 microregions
  // users/663zhuqx/MicroCentralWest
  // users/663zhuqx/MicroNorth
  // users/663zhuqx/MicroNorthEast 
  // users/663zhuqx/MicroSouth
  // users/663zhuqx/MicroSouthEast
var aois = ee.FeatureCollection("users/663zhuqx/MicroNorthEast");
Map.addLayer(aois);
var aois= aois.toList(aois.size());
print('aois',aois);



// Step 2:  Preparing epiweeks 
var startDate = ee.Date('2000-12-31'); // first day in first week  of 2013 
var endDate = ee.Date('2024-12-28'); // last day in last week of 2020
var dayOffsets_weekly = ee.List.sequence(0, endDate.difference(startDate, 'days'),7);  
print('epiweeks', dayOffsets_weekly);


//  Step 3:  Setting the weighting variable   
var pop_col = ee.ImageCollection([]);
for (var year = 2000; year <= 2020; year++) {
    var popWeight_img = ee.Image('WorldPop/GP/100m/pop/BRA_' + year)
        .set('year', year);
    pop_col = pop_col.merge(ee.ImageCollection([popWeight_img]));
}
// 添加2020-2024年使用2020年数据
for (var year = 2021; year <= 2024; year++) {
    var popWeight_img = ee.Image('WorldPop/GP/100m/pop/BRA_2020')
        .set('year', year);
    pop_col = pop_col.merge(ee.ImageCollection([popWeight_img]));
}



// Step 4: Computing the variables  
for (var i = 0 ; i < aois.size().getInfo(); i++){


var aoi = ee.Feature(aois.get(i));
var label = aoi.get('CD_MICRO').getInfo();  // CD_MICRO column name in the attribute table 




var pdsi = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE').filterDate(startDate, endDate.advance(1,'days')).select('pdsi');
var pdsi_rescale = pdsi.map(function(img) {
  return img.multiply(0.01)   // scaling -4 to 4
            .rename('pdsi')
            .set('system:time_start',img.get('system:time_start'));
});


var pdsi_ = ee.ImageCollection.fromImages(
  dayOffsets_weekly.map(function(dayOffset) {
    var start = startDate.advance(dayOffset, 'days');
    var end = start.advance(1, 'week');
    var year = start.get('year');
    var dayOfYear = start.getRelative('day', 'year');
    var filteredImgs = pdsi_rescale.filterDate(start, end);
    // finding the missing images per epiweek and replace them by a self-defined image with the value of -999
    var effectiveYear = ee.Number(year).min(2020);
    var popWeight = pop_col.filter(ee.Filter.eq('year', effectiveYear)).first();
    var composite = ee.Image(ee.Algorithms.If(filteredImgs.size().gt(0),filteredImgs.select('pdsi').sum(),ee.Image(-999).rename('pdsi'))).addBands(popWeight);
    return composite 
      .set('year', year)
      .set('day_of_year', dayOfYear)
      .set('date',start.format('YYYY-MM-dd'));
  }));




var fc_pdsi_mean = pdsi_.map(function(img){
  var dic = img.reduceRegion({
    reducer: ee.Reducer.mean().splitWeights(),
    scale: 4000,
    geometry: aoi.geometry(),
    maxPixels: 1e13,
    tileScale:4
  });
  return ee.Feature(null,dic).set('date',img.get('date'));
}).filter(ee.Filter.notNull(['mean']));


Export.table.toDrive(fc_pdsi_mean, 'pdsi_'+label, folder, null, 'CSV', ['date','mean']);

}