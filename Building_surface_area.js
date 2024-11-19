// Step 1:  Defining the folder for saving data 
var folder = 'gee_gain/built_surface_area';


// Step 2:  Preparing study area 
var aois = ee.FeatureCollection("users/microregions");
var aois = aois.toList(aois.size());


// Step 3:  Preparing epiweeks from 2013 to 2020 
// first day in first week of 2013 
// last day in last week of 2020
var startDate = ee.Date('2012-12-30');
var endDate = ee.Date('2021-01-02');
var dayOffsets_weekly = ee.List.sequence(0, endDate.difference(startDate, 'days'), 7);
print('epiweeks', dayOffsets_weekly);


// Load JRC/GHSL
var dataset = ee.ImageCollection("JRC/GHSL/P2023A/GHS_BUILT_S");

// Get 2010,2015,2020 data
var image2010 = dataset.filter(ee.Filter.calendarRange(2010, 2010, 'year')).first();
var image2015 = dataset.filter(ee.Filter.calendarRange(2015, 2015, 'year')).first();
var image2020 = dataset.filter(ee.Filter.calendarRange(2020, 2020, 'year')).first();

// Define a function that assigns image values ​to a given date range
function expandImageOverPeriod(image, startYear) {
  var startDate = ee.Date.fromYMD(startYear, 1, 1);
  var endDate = startDate.advance(5, 'year').advance(-1, 'day');

  // Creates an image collection containing images for each day within a specified date range
  var dateList = ee.List.sequence(0, endDate.difference(startDate, 'day').subtract(1));
  var images = dateList.map(function (dayOffset) {
    var date = startDate.advance(dayOffset, 'day');
    return image.set('system:time_start', date.millis())
      .copyProperties(image)
      .set('date', date.format('YYYY-MM-dd'));
  });
  return ee.ImageCollection(images);
}

//Expand imagery to a specified five-year range 
var images2010 = expandImageOverPeriod(image2010, 2010);
var images2015 = expandImageOverPeriod(image2015, 2015);
var images2020 = expandImageOverPeriod(image2020, 2020);

//Merge all image collections
var allImages = images2010.merge(images2015).merge(images2020);


// Step 4: Computing the variables 
for (var i = 0; i < aois.size().getInfo(); i++) {

  var aoi = ee.Feature(aois.get(i));
  var label = aoi.get('CD_MICRO').getInfo();
  print('Processing microregion:', label, 'Progress:', (i + 1) + '/' + aois.size().getInfo());

  //Select the GHSL image collection, 100 meter resolution, and calculate the weekly average 
  var GHSL = ee.ImageCollection(allImages).filterDate(startDate, endDate.advance(1, 'day')).select(['built_surface']);
  // Temporal composition 
  var built = ee.ImageCollection.fromImages(
    dayOffsets_weekly.map(function (dayOffset) {
      var start = startDate.advance(dayOffset, 'days');
      var end = start.advance(1, 'week');
      var year = start.get('year');
      var dayOfYear = start.getRelative('day', 'year');
      var filteredImgs = GHSL.filterDate(start, end);

      var composite = ee.Image(ee.Algorithms.If(
        filteredImgs.size().gt(0),
        filteredImgs.select('built_surface').mean(),
        ee.Image(-999)
      ));

      return composite
        .set('year', year)
        .set('day_of_year', dayOfYear)
        .set('date', start.format('YYYY-MM-dd'));
    })
  );

  // Spatial aggregation
  var results = ee.FeatureCollection(
    built.map(function (img) {
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


  //Export
  Export.table.toDrive({
    collection: results,
    description: 'built_' + label,
    folder: 'built_Export',
    fileFormat: 'CSV',
    selectors: ['date', 'built_sum']
  });
}
