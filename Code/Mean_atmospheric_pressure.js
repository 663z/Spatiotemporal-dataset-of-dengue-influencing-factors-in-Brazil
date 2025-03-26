// defining the folder for saving data 
var folder = 'atmospheric_pressure';


// Step 1:  Preparing study sites  

  // 5 zones, 560 microregions
  // users/663zhuqx/MicroCentralWest
  // users/663zhuqx/MicroNorth
  // users/663zhuqx/MicroNorthEast 
  // users/663zhuqx/MicroSouth
  // users/663zhuqx/MicroSouthEast
var aois = ee.FeatureCollection("users/663zhuqx/MicroCentralWest");
var aois= aois.toList(aois.size());


// Step 2:  Preparing epiweeks 
var startDate = ee.Date('2000-12-31'); // first day in first week  of 2001 
var endDate = ee.Date('2024-12-28'); // last day in last week of 2024
var dayOffsets_weekly = ee.List.sequence(0, endDate.difference(startDate, 'days'),7);  
print('epiweeks', dayOffsets_weekly);


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

//pressure, 11132 meters, hourly
var ERA5= ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY") 
  .filterDate(startDate,endDate.advance(1,'day'))
  .filterBounds(aoi.geometry())
  .select(['surface_pressure'],['pa']) 
var noDataImg = ERA5.first().select(['pa']).unmask(-999);

var ERA5_weekcomposite = ee.ImageCollection.fromImages(
  dayOffsets_weekly.map(function(dayOffset) {
    var start = startDate.advance(dayOffset, 'days');
    var end = start.advance(1, 'week');
    var year = start.get('year');
    var dayOfYear = start.getRelative('day', 'year');
    var filteredImgs = ERA5.select(['pa']).filterDate(start, end);
    // finding the missing images per epiweek and replace them by a self-defined image with noData
    var effectiveYear = ee.Number(year).min(2020);
    var popWeight = pop_col.filter(ee.Filter.eq('year', effectiveYear)).first();
    var composite = filteredImgs.select(['pa']).mean().addBands(popWeight);
    return composite                                                                               
      .set('year', year)
      .set('day_of_year', dayOfYear)
      .set('date',start.format('YYYY-MM-dd'));
  })  
);

// exporting and population-based weighting
var ERA5_pressure_mean = ERA5_weekcomposite.select(['pa','population']).map(function(img){
  var dic = img.reduceRegion({
    reducer: ee.Reducer.mean().splitWeights(),
    scale: 27000,
    geometry: aoi.geometry(),
    maxPixels: 1e13,
    tileScale:4
  });
  return ee.Feature(null,dic).set('date',img.get('date'));
}).filter(ee.Filter.notNull(['mean'])).map(function(f){
  return f.set('pa',f.get('mean'));
});

Export.table.toDrive(ERA5_pressure_mean, 'pressure_mean_'+label, folder, null, 'CSV', ['date','pa']);
}
