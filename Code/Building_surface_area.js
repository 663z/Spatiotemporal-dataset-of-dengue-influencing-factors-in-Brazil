// defining the folder for saving data 
var folder = 'built_surface_area';


// Step 1:  Preparing study sites 

  // 5 zones, 560 microregions
  // users/663zhuqx/MicroCentralWest
  // users/663zhuqx/MicroNorth
  // users/663zhuqx/MicroNorthEast 
  // users/663zhuqx/MicroSouth
  // users/663zhuqx/MicroSouthEast
var aois = ee.FeatureCollection("users/663zhuqx/MicroCentralWest");
var aois= aois.toList(aois.size());


// Step 2:  Preparing years 
var startDate = ee.Date('2001-01-01'); // first day in 2001 
var endDate = ee.Date('2024-12-31'); // last day in 2024
var nYears = endDate.difference(startDate, 'year');
var yearOffsets = ee.List.sequence(0, nYears); 


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
  
//Select the GHSL image , 100 meter resolution
  var built = ee.ImageCollection.fromImages(
    yearOffsets.map(function(yearOffset) {
      var start = startDate.advance(yearOffset, 'year');
      var end = start.advance(1, 'year');
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
  
  // Calculate
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
  
  
  // Export
  Export.table.toDrive({
    collection: results,
    description: 'built_' + label,
    folder: 'folder',
    fileFormat: 'CSV',
    selectors: ['year', 'built_sum']
  });
}

