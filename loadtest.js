var moment = require('moment');
var deepstream = require('deepstream.io-client-js')
var tz = require('moment-timezone');
var async = require('async')
const ds = deepstream(process.argv[3] + ':6020')
var _totalrecords = process.argv[2]
var _counter = 0
var message = {}
message.t = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus a venenatis odio, ac pharetra lacus. Donec scelerisque aliquet odio, ac dapibus erat feugiat non. Phasellus tempor rhoncus interdum. Donec semper volutpat dolor, non pharetra diam facilisis vitae. Fusce pretium et nisl semper pretium. Vivamus maximus odio accumsan eros malesuada vehicula. Donec sed ipsum sagittis, iaculis turpis eu, elementum eros. Nullam sit amet fringilla urna. Nam eleifend interdum purus vel venenatis. Pellentesque in ipsum nisl. Morbi id dictum ex, vitae facilisis diam. Ut maximus rutrum dictum.  Sed condimentum, erat eget venenatis suscipit, quam nulla dignissim nisi, in condimentum orci diam quis lectus. Nullam iaculis pulvinar euismod. Nullam porttitor, tortor eu fermentum facilisis, urna eros cursus risus, non feugiat tellus metus in enim. Suspendisse quis mauris augue. Nullam efficitur condimentum vulputate. Ut eget feugiat odio. Cras molestie massa at lacus interdum, sed posuere tortor pharetra. Curabitur urna est, ultricies eget lectus eu." //long message
message.producer = 'LoadTest' //short message
ds.on ('error', (error, event, topic) => {console.log(error, event, topic)})
ds.login({}, (success) => {
	var q = async.queue(function(task, callback) {
		record = ds.record.getRecord(task.name).set(task.record)
	}, 1 );
			var initTime = moment()
	q.drain = function (){
		console.log(_counter + ' items have finished processing')
	}
	for (i = 0; i <= _totalrecords-1; i++) {
		q.push({name: 'ds/loadTestBeta3/' + ds.getUid(), record: { id: i, createTimeStamp: moment().tz("America/Los_Angeles").format("YYYY-MM-DD hh:mm:ss.SSS"), consumer: 'None', consumerTimeStamp: moment().tz("America/Los_Angeles").format("YYYY-MM-DD hh:mm:ss.SSS"), t: message.t, producer: message.producer}
			}, function(err) {
				if(err) {console.log(err)}
				_counter = _counter + 1
				if (_counter % 500 == 0) {
					console.log(500/moment().diff(initTime, 'seconds', true) + ' TPS')
					console.log(_counter + ' messages processed')
					initTime = moment()
				}
			});
	}	
});