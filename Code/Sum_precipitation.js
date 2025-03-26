// defining the folder for saving data 
var folder = 'PR';


// Step 1:  Preparing study sites  

  // 5 zones, 560 microregions
  // users/663zhuqx/MicroCentralWest
  // users/663zhuqx/MicroNorth
  // users/663zhuqx/MicroNorthEast 
  // users/663zhuqx/MicroSouth
  // users/663zhuqx/MicroSouthEast
var aois = ee.FeatureCollection("users/663zhuqx/MicroSouthEast");
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
var pr_day = ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR") 
  .filterDate(startDate,endDate.advance(1,'day'))
  .filterBounds(aoi.geometry())
  .select(['total_precipitation_sum'])  
  .map(function(img){
      // scaling m to mm
      return img.addBands(img.select('total_precipitation_sum').multiply(1000).rename('pr')) 
  });

var noDataImg = pr_day.first().select(['pr']).unmask(-999);


var pr_weekcomposite = ee.ImageCollection.fromImages(
  dayOffsets_weekly.map(function(dayOffset) {
    var start = startDate.advance(dayOffset, 'days');
    var end = start.advance(1, 'week');
    var year = start.get('year');
    var dayOfYear = start.getRelative('day', 'year');
    var filteredImgs = pr_day.select(['pr']).filterDate(start, end);
    // finding the missing images per epiweek and replace them by a self-defined image with noData
    var effectiveYear = ee.Number(year).min(2020);
    var popWeight = pop_col.filter(ee.Filter.eq('year', effectiveYear)).first();    
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



// exporting and population-based weighting
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
