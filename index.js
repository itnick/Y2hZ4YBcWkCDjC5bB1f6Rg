'use strict';
// Require node modules
var config = require('config');
var fivebeans = require('fivebeans');
var request = require('request');
var cheerio = require('cheerio');
var mongodb = require('mongodb');
var sprintf = require("sprintf-js").sprintf;
var winston = require('winston');

// Load config
// Beanstalkd
var beanstalkd_config = config.get('beanstalkd');
var beanstalkd_client = new fivebeans.client(beanstalkd_config.host, beanstalkd_config.port);
var tube = beanstalkd_config.tube;
// Mongodb
var mongodb_config = config.get('mongodb');
var mongodb_client = new mongodb.MongoClient;
var mongodb_uri = mongodb_config.uri;
var mongodb_collection = mongodb_config.collection;
// Job config
var job_config = config.get('job');
var reput_priority = job_config.reput_priority;
var reput_delay_for_success = job_config.reput_delay_for_success;
var reput_delay_for_failure = job_config.reput_delay_for_failure;
var reput_ttr = job_config.reput_ttr;
// XE config
var xe_config = config.get('currency_exchange_vendors');
var xe_url = xe_config[0].url;
// Logger
var logger_level = config.get('logger_level');
var logger = new (winston.Logger)({
	transports: [
		new (winston.transports.Console)({colorize: true, level: logger_level})
	]
});

// Other variables
var number_of_success = 0;
var number_of_failure = 0;
var payload_string;
var mongo_database;

logger.debug('Process ID: %s', process.pid);

var exitProcessWithError = function() {
	process.exit(1);
};

// Connecting Beanstalkd
beanstalkd_client
	.on('connect', function()
	{
		// client can now be used
		logger.info('Beanstalkd is connected.');

	})
	.on('error', function(error)
	{
		// connection failure
		logger.error('Error occured when connecting Beanstalkd: %s', JSON.stringify(error));
		exitProcessWithError();
	})
	.on('close', function()
	{
		// underlying connection has closed
		logger.info('Beanstalkd connection is closed.');
	})
	.connect();


// Connecting to MongoDB
mongodb_client.connect(mongodb_uri, function(error, database) {

	if (!error) {
		mongo_database = database;

		// Use the specified tube
		beanstalkd_client
			.use(tube, function(err, tubename)
			{
				logger.info('Using tube: %s', tubename);
			});

		beanstalkd_client.watch(tube, function(err, numwatched) {
			reserveJob();
		});

		// Ignore default tube
		beanstalkd_client.ignore('default', function(err, numwatched) {});

	}
	else {

		logger.error('Error occured when connecting Mongodb: %s', JSON.stringify(error));
		exitProcessWithError();
		

	}
});






// Insert a document to MongoDB
var insertDocument = function(database, document, callback) {

	var collection = database.collection(mongodb_collection);
	collection.insert(document, function(err, result) {
		callback(result);
	});
};


// Processor
var processor = function(currency_from, currency_to, callback) {

	var url = sprintf(xe_url, currency_from, currency_to);
	logger.debug('url: %s', url);

	var request_options = {
		url: url,
		headers: {
			'User-Agent': 'request'
		},
		timeout: 10000
	};


	request(request_options, function(error, response, body) {

		// Check response
		logger.debug('response.statusCode: %s', response.statusCode);
		if (response.statusCode === 200) {

			// Parse
			var $ = cheerio.load(body);
			var left_currency = $('.uccRes .leftCol').text().replace('HKD', '');
			left_currency = parseFloat(left_currency).toFixed(2);
			logger.debug('left currency: %s', left_currency);
			var right_currency = $('.uccRes .rightCol').text().replace('USD', '');
			right_currency = parseFloat(right_currency).toFixed(2);
			logger.debug('right currency: %s', right_currency);

		} else {

			error = 'Response is not equal to 200.';

		}

		callback(error, right_currency);


		


	});


};



var reserveJob = function() {

	var document;

	var request_options = {
		url: xe_url,
		headers: {
			'User-Agent': 'request'
		}
	};

	// Reserve a job from beanstalkd
	beanstalkd_client.reserve(function(err, jobid, payload) {

		logger.debug('number_of_success: %d', number_of_success);
		logger.debug('number_of_failure: %d', number_of_failure);

		if (number_of_success === 1 || number_of_failure === 3) {
			mongo_database.close();
			beanstalkd_client.quit();
			process.exit();
		}

		payload_string = payload.toString();
		logger.debug('payload_string: %s', payload_string);

		// Process the payload
		processor('HKD', 'USD', function(error, exchange_rate) {

			if (!error) { // No error

				document = {
					"from": "HKD",
					"to": "USD",
					"created_at": new Date(),
					"rate": exchange_rate
				};

				insertDocument(mongo_database, document, function() {
					logger.info('Added a document to Mongodb.');
				});
				number_of_success++;

				// Reput to the tube
				beanstalkd_client.release(jobid, reput_priority, reput_delay_for_success, function(err) {
					reserveJob();
				});

			} else {

				logger.warn('Error occured while processing the payload: %s' + JSON.stringify(error));
				number_of_failure++;

				// Reput to the tube
				beanstalkd_client.release(jobid, reput_priority, reput_delay_for_failure, function(err) {
					reserveJob();
				});

			}



		});



		
	});

};