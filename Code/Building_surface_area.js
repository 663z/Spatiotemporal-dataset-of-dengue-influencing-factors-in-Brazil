// defining the folder for saving data 
var folder = 'gee_gain/built_surface_area';


// Step 1:  Preparing study sites  

  // 5 zones, 560 microregions
  // users/663zhuqx/MicroCentralWest
  // users/663zhuqx/MicroNorth
  // users/663zhuqx/MicroNorthEast 
  // users/663zhuqx/MicroSouth
  // users/663zhuqx/MicroSouthEast
var aois = ee.FeatureCollection("users/663zhuqx/MicroNorth");
var aois= aois.toList(aois.size());


// Step 2:  Preparing epiweeks 
var startDate = ee.Date('2000-12-31'); // first day in first week  of 2001
var endDate = ee.Date('2024-12-28'); // last day in last week of 2024
var dayOffsets_weekly = ee.List.sequence(0, endDate.difference(startDate, 'days'),7);  
print('epiweeks', dayOffsets_weekly);


// load JRC/GHSL
var dataset = ee.ImageCollection("JRC/GHSL/P2023A/GHS_BUILT_S");

// Getting data for a specified year
function getImageForYear(year) {
  var image = dataset.filter(ee.Filter.calendarRange(year, year, 'year')).first();
  return image;
}

// Step 3: Computing the variables 
for (var i = 0; i < aois.size().getInfo(); i++) {

  var aoi = ee.Feature(aois.get(i));
  var label = aoi.get('CD_MICRO').getInfo();
  
  print('Processing microregion:', label, 'Progress:', (i + 1) + '/' + aois.size().getInfo());
  
  //Selecting the GHSL image , 100 meter resolution
  var built = ee.ImageCollection.fromImages(
    dayOffsets_weekly.map(function(dayOffset) {
      var start = startDate.advance(dayOffset, 'days');
      var end = start.advance(1, 'week');
      var year = start.get('year');
      var dayOfYear = start.getRelative('day', 'year');
      var targetYear = ee.Algorithms.If(
        year.lt(2005), 2000,
        ee.Algorithms.If(
          year.lt(2010), 2005,
          ee.Algorithms.If(
            year.lt(2015), 2010,
            ee.Algorithms.If(
              year.lt(2020), 2015,
              2020
            )
          )
        )
      );
      
      var image = getImageForYear(targetYear);
      var filteredImgs = ee.Image(image).select('built_surface');
      return filteredImgs
        .set('year', year)
        .set('day_of_year', dayOfYear)
        .set('date', start.format('YYYY-MM-dd'));
    })
  );
  
  // Calculating the sum of all pixels in the AOI every week and generate a FeatureCollection
  var results = ee.FeatureCollection(
    built.map(function(img) {
      var sum = img.reduceRegion({
        reducer: ee.Reducer.sum(),
        geometry: aoi.geometry(),
        scale: 100,
        maxPixels: 1e13,
        tileScale: 4
      });
      
      return ee.Feature(null, {
        'date': img.get('date'),
        'built_sum': sum.get('built_surface'),
        'year': img.get('year'),
        'day_of_year': img.get('day_of_year')
      }).setGeometry(null);
    }).filter(ee.Filter.notNull(['built_sum']))
  );
  
  
  // export
  Export.table.toDrive({
    collection: results,
    description: 'built_' + label,
    folder: 'folder',
    fileFormat: 'CSV',
    selectors: ['date', 'built_sum']
  });
}
