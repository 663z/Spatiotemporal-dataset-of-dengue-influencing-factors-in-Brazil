// defining the folder for saving data 
var folder = 'WindSpeed';


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


// wind speed v and u, 11,132 m, hourly
var wind = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY')
  .filterDate(startDate,endDate.advance(1,'day'))
  .filterBounds(aoi.geometry())
  .select(['u_component_of_wind_10m','v_component_of_wind_10m'])
  .map(function(img){
      return img.addBands(img.expression('sqrt(V**2 + U**2)', 
                          {
                            'V':  ee.Image(img).select('v_component_of_wind_10m'),
                            'U':  ee.Image(img).select('u_component_of_wind_10m'),
                          }).float().rename('wind_speed'));
  });




var noDataImg = wind.first().select(['wind_speed']).unmask(-999);




var wind_weekcomposite = ee.ImageCollection.fromImages(
  dayOffsets_weekly.map(function(dayOffset) {
    var start = startDate.advance(dayOffset, 'days');
    var end = start.advance(1, 'week');
    var year = start.get('year');
    var dayOfYear = start.getRelative('day', 'year');
    var filteredImgs = wind.select(['wind_speed']).filterDate(start, end);
    // finding the missing images per epiweek and replace them by a self-defined image with noData
    var effectiveYear = ee.Number(year).min(2020);
    var popWeight = pop_col.filter(ee.Filter.eq('year', effectiveYear)).first();
    var composite = ee.Image(ee.Algorithms.If(
      filteredImgs.size().gt(0),
      ee.Image([
          filteredImgs.select(['wind_speed']).mean()
        ]),
      noDataImg)).addBands(popWeight);
    return composite                                                                               
      .set('year', year)
      .set('day_of_year', dayOfYear)
      .set('date',start.format('YYYY-MM-dd'));
  })  
);

// exporting and population-based weighting
var wind_mean = wind_weekcomposite.select(['wind_speed','population']).map(function(img){
  var dic = img.reduceRegion({
    reducer: ee.Reducer.mean().splitWeights(),
    scale: 27000,
    geometry: aoi.geometry(),
    maxPixels: 1e13,
    tileScale:4
  });
  return ee.Feature(null,dic).set('date',img.get('date'));
}).filter(ee.Filter.notNull(['mean'])).map(function(f){
  return f.set('wind_speed',f.get('mean'));
});

Export.table.toDrive(wind_mean, 'WindSpeed_mean_'+label, folder, null, 'CSV', ['date','wind_speed']);

}
