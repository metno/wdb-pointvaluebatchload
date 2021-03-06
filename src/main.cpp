/*
    fastload

    Copyright (C) 2012 met.no

    Contact information:
    Norwegian Meteorological Institute
    Box 43 Blindern
    0313 OSLO
    NORWAY
    E-mail: post@met.no

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
    MA  02110-1301, USA
*/


#include "CopyJob.h"
#include "TranslateJob.h"
#include "Configuration.h"

#include <log4cpp/Appender.hh>
#include <log4cpp/Category.hh>
#include <log4cpp/CategoryStream.hh>
#include <log4cpp/RollingFileAppender.hh>
#include <log4cpp/OstreamAppender.hh>

#include <boost/thread/thread.hpp>
#include <boost/program_options.hpp>
#include <boost/scoped_ptr.hpp>
#include <iostream>
#include <fstream>


namespace
{
void checkForErrors(const fastload::AbstractJob & job)
{
	if ( job.status() == fastload::AbstractJob::Error )
	{
		log4cpp::Category & log = log4cpp::Category::getInstance( "wdb.fastload.job" );
		log.fatal(job.errorMessage());
		exit(1);
	}
}

void copyData(std::istream & source, fastload::DataQueue & sink)
{
	std::string line;
	while ( std::getline(source, line) )
		sink.put(line);
}

log4cpp::Appender * getLogAppender(const fastload::Configuration & configuration)
{
	const std::string & fileName = configuration.logFile();
	if ( fileName.empty() ) // log to stdout
		return new log4cpp::OstreamAppender( "TheLogger", & std::clog );

	return new log4cpp::RollingFileAppender( "TheLogger", fileName, 1024 * 1024, 10 );
}
}


/**
 * converts a pointer to a function object into a proper function object
 */
template<typename T>
class pointer_runner
{
public:
	typedef boost::shared_ptr<T> value_type;

	explicit pointer_runner(value_type toRun) :
			toRun_(toRun)
	{}

	void operator() ()
	{
		(*toRun_)();
	}
private:
	value_type toRun_;
};


int main(int argc, char ** argv)
{
	try
	{
		boost::shared_ptr<fastload::Configuration> configuration;
		try
		{
			configuration = boost::shared_ptr<fastload::Configuration>(new fastload::Configuration(argc, argv));

			log4cpp::Appender * appender(getLogAppender(* configuration));
			log4cpp::Category & log = log4cpp::Category::getInstance("wdb");
			log.addAppender(appender);
			log.setPriority(configuration->logLevel());
		}
		catch ( std::exception & e )
		{
			// Failure to read configuration or set up logging will be written
			// to stderr instead of logs:
			std::clog << e.what() << std::endl;
			return 1;
		}
		log4cpp::Category & log = log4cpp::Category::getInstance("wdb");

		fastload::DataQueue::Ptr rawQue(new fastload::DataQueue(50000, "raw"));
		fastload::DataQueue::Ptr translatedQue(new fastload::DataQueue(1000, "translated"));

		fastload::TranslateJob::Ptr translateJob = fastload::TranslateJob::get(configuration->pqConnect(), configuration->wciUser(), configuration->nameSpace(), rawQue, translatedQue, not configuration->onlyCreateCroups());
		pointer_runner<fastload::TranslateJob> translateRunner(translateJob);
		boost::thread translateThread(translateRunner);

		fastload::old::CopyJob copyJob(configuration->pqConnect(), translatedQue);
		boost::thread copyThread(copyJob);

		try
		{
			for ( std::vector<std::string>::const_iterator it = configuration->file().begin(); it != configuration->file().end(); ++ it )
			{
				if ( * it == "-" )
					copyData(std::cin, * rawQue);
				else
				{
					std::ifstream s(it->c_str());
					if ( ! s.good() )
					{
						log.fatalStream() << "Unable to open file " << * it;
						exit(1);
					}
					copyData(s, * rawQue);
				}
			}
			rawQue->done();
		}
		catch ( std::exception & e )
		{
			// improved error logging
			checkForErrors(* translateJob);
			checkForErrors(copyJob);

			log.fatal(e.what());
			return 1;
		}

		const boost::posix_time::time_duration duration(0,0,1);
		while ( translateJob->status() != fastload::AbstractJob::Done or copyJob.status() != fastload::AbstractJob::Done )
		{
			checkForErrors(* translateJob);
			checkForErrors(copyJob);
			boost::this_thread::sleep(duration);
		}

		log.infoStream() << "COPY " << translatedQue->callsToGet();
	}
	catch ( std::exception & e )
	{
		log4cpp::Category::getInstance("wdb").fatal(e.what());
		return 1;
	}
}
