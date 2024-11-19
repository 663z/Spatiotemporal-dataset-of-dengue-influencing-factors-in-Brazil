// Step 1:  Defining the folder for saving data 
var folder = 'gee_gain/Brazil_microregion_pdsi';

// Step 2:  Preparing study area  
var aois = ee.FeatureCollection("users/microregions");
Map.addLayer(aois);
var aois = aois.toList(aois.size());
print('aois', aois);



// Step 3:  Preparing epiweeks from 2013 to 2020 
// first day in first week of 2013 
// last day in last week of 2020
var startDate = ee.Date('2012-12-30');
var endDate = ee.Date('2021-01-02');
var dayOffsets_weekly = ee.List.sequence(0, endDate.difference(startDate, 'days'), 7);
print('epiweeks', dayOffsets_weekly);


// Step 4:  Considering population counts as the weights of spatial aggregation
var popWeight = ee.Image('WorldPop/GP/100m/pop/BRA_2017');
print(popWeight);



// Step 5: Computing the variables 
for (var i = 0; i < aois.size().getInfo(); i++) {

  // CD_MICRO: column name in the attribute table 
  var aoi = ee.Feature(aois.get(i));
  var label = aoi.get('CD_MICRO').getInfo();
  print(label);




  // TerraClimate, monthly, 4638.3m 
  var pdsi = ee.ImageCollection('IDAHO_EPSCOR/TERRACLIMATE').filterDate(startDate, endDate.advance(1, 'days')).select('pdsi');
  var pdsi_rescale = pdsi.map(function (img) {
    return img.multiply(0.0001)   // scaling -4 to 4
      .rename('pdsi')
      .set('system:time_start', img.get('system:time_start'));
  });




  // Temporal composition 
  var pdsi_ = ee.ImageCollection.fromImages(
    dayOffsets_weekly.map(function (dayOffset) {
      var start = startDate.advance(dayOffset, 'days');
      var end = start.advance(1, 'week');
      var year = start.get('year');
      var dayOfYear = start.getRelative('day', 'year');
      var filteredImgs = pdsi_rescale.filterDate(start, end);
      // finding the missing images per epiweek and replace them by a self-defined image with the value of -999
      var composite = ee.Image(ee.Algorithms.If(filteredImgs.size().gt(0), filteredImgs.select('pdsi').mean(), ee.Image(-999).rename('pdsi'))).addBands(popWeight);
      return composite
        .set('year', year)
        .set('day_of_year', dayOfYear)
        .set('date', start.format('YYYY-MM-dd'));
    }));



  // Population-based weighted spatial aggregation
  var fc_pdsi_mean = pdsi_.map(function (img) {
    var dic = img.reduceRegion({
      reducer: ee.Reducer.mean().splitWeights(),
      scale: 4000,
      geometry: aoi.geometry(),
      maxPixels: 1e13,
      tileScale: 4
    });
    return ee.Feature(null, dic).set('date', img.get('date'));
  }).filter(ee.Filter.notNull(['mean']));


  Export.table.toDrive(fc_pdsi_mean, 'pdsi_mean_' + label, folder, null, 'CSV', ['date', 'mean']);

}