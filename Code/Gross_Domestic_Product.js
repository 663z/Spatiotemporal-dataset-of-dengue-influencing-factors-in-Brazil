// defining the folder for saving data 
var folder = 'GDP';


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
var endDate = ee.Date('2019-12-31'); // last day in 2019
var nYears = endDate.difference(startDate, 'year');
var yearOffsets = ee.List.sequence(0, nYears); 

var dataset = ee.ImageCollection("projects/sat-io/open-datasets/GRIDDED_EC-GDP");

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
  
  // 1.Select the GDP image collection, 1000 meter resolution, and calculate weekly averages
  var GDP = ee.ImageCollection.fromImages(
    yearOffsets.map(function(yearOffset) {
      var start = startDate.advance(yearOffset, 'year');
      var end = start.advance(1, 'year');
      var year = start.get('year');
      
      var image = getImageForYear(year);
      var filteredImgs = ee.Image(image).select('b1');
      return filteredImgs
        .set('year', year)
    })
  );
  
  // 2. Calculate the sum of all pixels 
  var results = ee.FeatureCollection(
    GDP.map(function(img) {
      var sum = img.reduceRegion({
        reducer: ee.Reducer.sum(),
        geometry: aoi.geometry(),
        scale: 1000,
        maxPixels: 1e13,
        tileScale: 4
      });
      
      return ee.Feature(null, {
        'GDP_sum': sum.get('b1'),
        'year': img.get('year'),
      }).setGeometry(null);
    }).filter(ee.Filter.notNull(['GDP_sum']))
  );
  
  
  // 3. Export
  Export.table.toDrive({
    collection: results,
    description: 'GDP_' + label,
    folder: 'GDP',
    fileFormat: 'CSV',
    selectors: ['year', 'GDP_sum']
  });
}
