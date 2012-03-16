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
		std::clog << job.errorMessage() << std::endl;
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


	fastload::DataQue::Ptr rawQue(new fastload::DataQue(50000, false));
	fastload::DataQue::Ptr translatedQue(new fastload::DataQue(1000, false));

	fastload::TranslateJob translateJob(configuration.database().pqDatabaseConnection(), configuration.nameSpace, rawQue, translatedQue);
	boost::thread translateThread(translateJob);

	fastload::CopyJob copyJob(configuration.database().pqDatabaseConnection(), translatedQue);
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
				std::clog << "Error when opening file " << * it << std::endl;
				exit(1);
			}
			copyData(s, * rawQue);
		}
	}
	rawQue->done();

	translateThread.join();
	checkForErrors(translateJob);

	copyThread.join();
	checkForErrors(copyJob);

	std::cout << "COPY " << translatedQue->callsToGet() << std::endl;
}
