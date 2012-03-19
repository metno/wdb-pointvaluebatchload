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
#include <wdbLogHandler.h>
#include <boost/thread/thread.hpp>
#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>


namespace
{
void checkForErrors(const fastload::AbstractJob & job)
{
	if ( job.status() == fastload::AbstractJob::Error )
	{
		WDB_LOG & log = WDB_LOG::getInstance( "wdb.fastload.job" );
		log.error(job.errorMessage());
		exit(1);
	}
}

void version(std::ostream & s)
{
	s << "fastload (0.1.0)" << std::endl;
}
void help(std::ostream & s, const wdb::WdbConfiguration & configuration)
{
	version(s);
	s << '\n';
	s << "Load point data into a wdb database.\n";
	s << '\n';
	s << "This program is meant for (relatively) fast loading of large amounts of data.\n"
			"This is done by reading input from stdin. The format for this is described\n"
			"below.\n";
	s << '\n';
	s << "Normally, if errors are encountered, the program will merely log a warning and\n"
			"attempt to continue as if nothing has happened. This behavior may be\n"
			"altered by using the --all-or-nothing command-line switch, which will cause\n"
			"any error to be fatal, and revert all loading that has been done so far.\n";
	s << '\n';
	fastload::Configuration::printFormatHelp(s);
	s << '\n';
	s << configuration.shownOptions() << std::endl;
}

void copyData(std::istream & source, fastload::DataQue & sink)
{
	std::string line;
	while ( std::getline(source, line) )
		sink.put(line);
}
}


int main(int argc, char ** argv)
{
	fastload::Configuration configuration;
	configuration.parse(argc, argv);

	if ( configuration.general().help )
	{
		help(std::cout, configuration);
		exit(0);
	}
	if ( configuration.general().version )
	{
		version(std::cout);
		exit(0);
	}

	wdb::WdbLogHandler logHandler( configuration.logging().loglevel, configuration.logging().logfile );
	WDB_LOG & log = WDB_LOG::getInstance( "wdb.fastload.main" );
	log.debug( "Starting fastload" );

	fastload::DataQue::Ptr rawQue(new fastload::DataQue(50000, "raw"));
	fastload::DataQue::Ptr translatedQue(new fastload::DataQue(1000, "translated"));

	fastload::TranslateJob translateJob(configuration.database().pqDatabaseConnection(), configuration.nameSpace, rawQue, translatedQue, configuration.allOrNothing);
	boost::thread translateThread(translateJob);

	fastload::CopyJob copyJob(configuration.database().pqDatabaseConnection(), translatedQue, configuration.allOrNothing);
	boost::thread copyThread(copyJob);

	if ( configuration.file.empty() )
		configuration.file.push_back("-");
	for ( std::vector<std::string>::const_iterator it = configuration.file.begin(); it != configuration.file.end(); ++ it )
	{
		if ( * it == "-" )
			copyData(std::cin, * rawQue);
		else
		{
			std::ifstream s(it->c_str());
			if ( ! s.good() )
			{
				log.errorStream() << "Unable to open file " << * it;
				exit(1);
			}
			copyData(s, * rawQue);
		}
	}
	rawQue->done();

	const boost::posix_time::time_duration duration(0,0,1);
	while ( true )
	{
		if ( translateThread.timed_join(duration) )
			checkForErrors(translateJob);
		if ( copyThread.timed_join(duration) )
			checkForErrors(copyJob);
	}

	log.infoStream() << "COPY " << translatedQue->callsToGet();
}
