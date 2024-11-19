// Step 1:  Defining the folder for saving data 
var folder = 'gee_gain/GDP';

// Step 2:  Preparing study area 
var aois = ee.FeatureCollection("users/microregions");
var aois= aois.toList(aois.size());


// Step 3:  Preparing epiweeks from 2013 to 2020 
// first day in first week of 2013 
// last day in last week of 2020
var startDate = ee.Date('2012-12-30'); 
var endDate = ee.Date('2019-12-28'); 
var dayOffsets_weekly = ee.List.sequence(0, endDate.difference(startDate, 'days'),7); 


var GDPCollection = ee.ImageCollection("projects/sat-io/open-datasets/GRIDDED_EC-GDP")
  .filterDate(startDate, endDate.advance(1, 'day')) 
  .select(['b1']); 
  
var dailyGDPImages = ee.ImageCollection.fromImages(
  ee.List.sequence(2013, 2019).map(function(year) {
    var annualGDP = GDPCollection.filter(ee.Filter.calendarRange(year, year, 'year')).first();
    
    // Check if the year is a leap year
    function isLeapYear(year) {
      var yearNum = ee.Number(year);
      return yearNum.mod(4).eq(0).and(yearNum.mod(100).neq(0)).or(yearNum.mod(400).eq(0));
    }

    var daysInYear = isLeapYear(year) ? 365 : 364;
    return ee.List.sequence(0, daysInYear - 1).map(function(dayOfYear) {
      var date = ee.Date.fromYMD(year, 1, 1).advance(dayOfYear, 'days');
      return annualGDP.rename('GDP').set('system:time_start', date.millis());
    });
  }).flatten()
);


// Step 4: Computing the variables 
for (var i = 0; i < aois.size().getInfo(); i++) {

  var aoi = ee.Feature(aois.get(i));
  var label = aoi.get('CD_MICRO').getInfo();  
  
  print('Processing microregion:', label, 'Progress:', (i + 1) + '/' + aois.size().getInfo());
  
  // Select the GDP image collection, 1000 meter resolution, and calculate weekly averages
  var GDP = ee.ImageCollection(dailyGDPImages).filterDate(startDate, endDate.advance(1, 'day')).select(['GDP']);
  // Temporal composition
  var GDP_ = ee.ImageCollection.fromImages(
    dayOffsets_weekly.map(function(dayOffset) {
      var start = startDate.advance(dayOffset, 'days');
      var end = start.advance(1, 'week');
      var year = start.get('year');
      var dayOfYear = start.getRelative('day', 'year');
      var filteredImgs = GDP.filterDate(start, end);
      
      var composite = ee.Image(ee.Algorithms.If(
        filteredImgs.size().gt(0), 
        filteredImgs.select('GDP').mean(), 
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
    GDP_.map(function(img) {
      var sum = img.reduceRegion({
        reducer: ee.Reducer.sum(),
        geometry: aoi.geometry(),
        scale: 1000,
        maxPixels: 1e13,
        tileScale: 4
      });
      
      return ee.Feature(null, {
        'date': img.get('date'),
        'GDP_sum': sum.get('GDP'),
        'year': img.get('year'),
        'day_of_year': img.get('day_of_year')
      }).setGeometry(null);
    }).filter(ee.Filter.notNull(['GDP_sum']))
  );
  
  
  // Export
  Export.table.toDrive({
    collection: results,
    description: 'GDP_' + label,
    folder: 'GDP_Export',
    fileFormat: 'CSV',
    selectors: ['date', 'GDP_sum']
  });
}