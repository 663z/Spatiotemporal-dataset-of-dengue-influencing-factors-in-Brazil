// Step 1:  Defining the folder for saving data 
var folder = 'gee_gain/WindSpeed';

// Step 2:  Preparing study area 
var aois = ee.FeatureCollection("uusers/microregions");
var aois= aois.toList(aois.size());

// Step 3:  Preparing epiweeks from 2013 to 2020 
// first day in first week of 2013 
// last day in last week of 2020
var startDate = ee.Date('2012-12-30'); 
var endDate = ee.Date('2021-01-02');
var dayOffsets_weekly = ee.List.sequence(0, endDate.difference(startDate, 'days'),7);  

// Step 4:  Considering population counts as the weights of spatial aggregation
var popWeight = ee.Image('WorldPop/GP/100m/pop/BRA_2017');


// Step 5: Computing the variables 
for (var i = 0 ; i < aois.size().getInfo(); i++){

// CD_MICRO: column name in the attribute table 
var aoi = ee.Feature(aois.get(i));
var label = aoi.get('CD_MICRO').getInfo(); 
//print(label);

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




// Temporal composition 
var wind_weekcomposite = ee.ImageCollection.fromImages(
  dayOffsets_weekly.map(function(dayOffset) {
    var start = startDate.advance(dayOffset, 'days');
    var end = start.advance(1, 'week');
    var year = start.get('year');
    var dayOfYear = start.getRelative('day', 'year');
    var filteredImgs = wind.select(['wind_speed']).filterDate(start, end);
    // finding the missing images per epiweek and replace them by a self-defined image with noData
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

// Population-based weighted spatial aggregation
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