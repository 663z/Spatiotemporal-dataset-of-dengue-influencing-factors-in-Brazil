// Step 1:  Defining the folder for saving data 
var folder = 'gee_gain/RH';


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

print('Processing microregion:', label, 'Progress:', (i + 1) + '/' + aois.size().getInfo()); 



//rh
var ERA5_land = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY')
  .filterDate(startDate,endDate.advance(1,'day'))
  .filterBounds(aoi.geometry())
  .select(['dewpoint_temperature_2m','temperature_2m'])
  .map(function(img){
    var dewtemp = img.select('dewpoint_temperature_2m').subtract(273.15).rename('dewtemp');
    var temp = img.select('temperature_2m').subtract(273.15).rename('temp');
    
    // Expression to calculate relative humidity
    var rh = img.expression(
      '100 * (exp((17.67 * Td) / (243.5 + Td)) / exp((17.67 * T) / (243.5 + T)))',
      {
        'Td': dewtemp,
        'T': temp
      }
    ).float().rename('rh');
    return img.addBands(dewtemp).addBands(temp).addBands(rh);
  });
var noDataImg = ERA5_land.first().select(['rh']).unmask(-999);

// Temporal composition 
var ERA5_weekcomposite = ee.ImageCollection.fromImages(
  dayOffsets_weekly.map(function(dayOffset) {
    var start = startDate.advance(dayOffset, 'days');
    var end = start.advance(1, 'week');
    var year = start.get('year');
    var dayOfYear = start.getRelative('day', 'year');
    var filteredImgs = ERA5_land.select(['rh']).filterDate(start, end);
    // finding the missing images per epiweek and replace them by a self-defined image with noData
    var composite = ee.Image(ee.Algorithms.If(
      filteredImgs.size().gt(0),
      ee.Image([
          filteredImgs.select(['rh']).mean()
        ]),
      noDataImg)).addBands(popWeight);
    return composite                                                                               
      .set('year', year)
      .set('day_of_year', dayOfYear)
      .set('date',start.format('YYYY-MM-dd'));
  })  
);

// Population-based weighted spatial aggregation
var fc_rh = ERA5_weekcomposite.select(['rh','population']).map(function(img){
  var dic = img.reduceRegion({
    reducer: ee.Reducer.mean().splitWeights(),
    scale: 27000,
    geometry: aoi.geometry(),
    maxPixels: 1e13,
    tileScale:4
  });
  return ee.Feature(null,dic).set('date',img.get('date'));
}).filter(ee.Filter.notNull(['mean'])).map(function(f){
  return f.set('rh',f.get('mean'));
});

Export.table.toDrive(fc_rh, 'rh_mean_'+label, folder, null, 'CSV', ['date','rh']);
}