var csv = require('fast-csv'),
     geocode = require('timsgeocoder'),
     fs = require('fs'),
     RateLimiter = require('limiter').RateLimiter;
     
var FILENAME_IN = 'jos_pollingplaces.csv',
     FILENAME_OUT = 'geocoded.csv',
     limiter = new RateLimiter(3, 'second'),
     rows = [],
     errors = [],
     pending = 0;

// Read file
fs.createReadStream(FILENAME_IN)
.pipe(csv({
	headers: true,
	ignoreEmpty: true
}))
.on('data', function(row) {
     // Add each row to the rows array
	rows.push(row);
})
.on('end', function() {
     rows.slice(0, 3).forEach(function(row) {
          // Increment # of pending requests
          pending++;
          
          // Execute this function when we have an available token from the rate limiter
          limiter.removeTokens(1, function(err, remainingRequests) {
               
               // Geocode this row
	          geocode(row.pin_address + ', Philadelphia, PA ' + row.zip_code, 'google', function(err, result) {
	               if(err || ! result) {
	                    console.log('***Error: ', row.pin_address);
	                    errors.push(row.pin_address); // Add address to errors array for manual post-processing
	               }
	               // Ignore results where google's `location_type` is 'APPROXIMATE'
	               // https://developers.google.com/maps/documentation/geocoding/#Results
	               else if(result.location_type === 'APPROXIMATE') {
	                    console.log('***Approximate: ', row.pin_address, ' (Skipping)');
	                    errors.push(row.pin_address); // Add address to errors array for manual post-processing
	               } else {
	                    // Update lat & lng values on the row, which updates it in the rows array
                         row.lat = result.lat;
                         row.lng = result.lng;
                         console.log('Success: ', row.pin_address, row.lat, row.lng);
	               }
	               // Decrement the # of pending requests since this one is done
	               pending--;
	               
	               // Check if this is the last request
	               checkPending();
	          });
	     });
     });
});

// Gets called after each request completes
// Check if # of pending requests is 0 and write the data to a file
var checkPending = function() {
     if(pending === 0) {
          console.log('No more pending results');
          if(errors.length) {
               console.log("Errors:\n" + errors.join("\n"));
          } else {
               console.log('No errors');
          }
          //var outFile = fs.createWriteStream(FILENAME_OUT);
          //csv.write(rows, {headers: true}).pipe(outFile);
     }
}