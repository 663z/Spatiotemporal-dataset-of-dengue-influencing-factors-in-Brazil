// Step 1:  Defining the folder for saving data 
var folder = 'gee_gain/PR_sum';


// Step 2:  Preparing study area 
var aois = ee.FeatureCollection("users/microregions");
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
var pr_day = ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR") 
  .filterDate(startDate,endDate.advance(1,'day'))
  .filterBounds(aoi.geometry())
  .select(['total_precipitation_sum'])  
  .map(function(img){
      // scaling m to mm
      return img.addBands(img.select('total_precipitation_sum').multiply(1000).rename('pr')) 
  });
var noDataImg = pr_day.first().select(['pr']).unmask(-999);

// Temporal composition 
var pr_weekcomposite = ee.ImageCollection.fromImages(
  dayOffsets_weekly.map(function(dayOffset) {
    var start = startDate.advance(dayOffset, 'days');
    var end = start.advance(1, 'week');
    var year = start.get('year');
    var dayOfYear = start.getRelative('day', 'year');
    var filteredImgs = pr_day.select(['pr']).filterDate(start, end);
    // finding the missing images per epiweek and replace them by a self-defined image with noData
    var composite = ee.Image(ee.Algorithms.If(
      filteredImgs.size().gt(0),
      ee.Image([
          filteredImgs.select(['pr']).sum()
        ]),
      noDataImg)).addBands(popWeight);
    return composite                                                                               
      .set('year', year)
      .set('day_of_year', dayOfYear)
      .set('date',start.format('YYYY-MM-dd'));
  })  
);



// Population-based weighted spatial aggregation
var pr_mean = pr_weekcomposite.select(['pr','population']).map(function(img){
  var dic = img.reduceRegion({
    reducer: ee.Reducer.mean().splitWeights(),
    scale: 11000,
    geometry: aoi.geometry(),
    maxPixels: 1e13,
    tileScale:4
  });
  return ee.Feature(null,dic).set('date',img.get('date'));
}).filter(ee.Filter.notNull(['mean'])).map(function(f){
  return f.set('pr',f.get('mean'));
});


Export.table.toDrive(pr_mean, 'pr_sum_'+label, folder, null, 'CSV', ['date','mean']);

}