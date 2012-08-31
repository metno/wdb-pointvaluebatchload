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

#include "Configuration.h"
#include <boost/program_options.hpp>
#include <sstream>
#include <iostream>

using namespace boost::program_options;


namespace fastload
{

Configuration::Configuration(int argc, char ** argv)
{
	parseOptions_(argc, argv);
}

Configuration::~Configuration()
{
}

std::string Configuration::pqConnect() const
{
	std::ostringstream ret;
	ret << "dbname=" << database_.c_str() << " ";
	if ( not host_.empty() )
		ret << "host=" << host_.c_str() << " ";
	if ( port_ )
		ret << "port=" << port_ << " ";
	if ( not user_.empty() )
		ret << "user=" << user_.c_str();
	return ret.str();
}

log4cpp::Priority::Value Configuration::logLevel() const
{
	using log4cpp::Priority;

	switch ( logLevel_ )
	{
	case 1:
		return Priority::DEBUG;
	case 2:
		return Priority::INFO;
	case 3:
		return Priority::WARN;
	case 4:
		return Priority::ERROR;
	case 5:
		return Priority::FATAL;
	default:
		std::ostringstream msg;
		msg << "Invalid value for loglevel: " << logLevel_;
		throw std::runtime_error(msg.str());
	}
}

std::ostream & Configuration::printVersion(std::ostream & s)
{
	return s << PACKAGE_STRING << std::endl;
}

std::ostream & Configuration::printHelp(std::ostream & s)
{
	printVersion(s);
	s << '\n';
	s << "Load point data into a wdb database.\n";
	s << '\n';
	s << "This program is meant for (relatively) fast loading of large amounts of data.\n"
			"This is done by reading input from stdin. The format for this is described\n"
			"below.\n";
	s << '\n';
	s << "Note that this program runs as a single transaction. This means that if\n"
			"loading of a single row fails, nothing will be added to the database.\n";
	s << '\n';
	fastload::Configuration::printFormatHelp(s);
	s << '\n';
	s << shownOptions_() << std::endl;
}


std::ostream & Configuration::printFormatHelp(std::ostream & s)
{
	s <<			"Input format for loading point data into wdb\n"
					"\n"
					"When loading data, you need to follow the format described here:\n"
					"\n"
					"All files consists of sections, starting with a data provider name on\n"
					"one line, optionally followed by a tab character and a three-part namespace\n"
					"specification. Data listings should follow on the next lines, with no blank\n"
					"lines between data elements.\n"
					"\n"
					"The \"current\" data provider is reset after a blank line has been\n"
					"encountered. The next line is then expected to contain the name of the\n"
					"next data provider to use.\n"
					"\n"
					"The data listings follow the same pattern as the wci.write function\n"
					"call parameters, with a few exceptions: It should contain instructions\n"
					"for inserting a single tuple on each line. The elements on each line\n"
					"should not contain any quotation marks. Also, the separator between\n"
					"fields should be a single tab character.\n"
					"\n"
					"Here is the ordering of elements in the data list:\n"
					"\n"
					"  * value \n"
					"  * place name \n"
					"  * reference time \n"
					"  * valid from \n"
					"  * valid to\n"
					"  * value parameter name\n"
					"  * level parameter name\n"
					"  * level from\n"
					"  * level to\n"
					"  \n"
					"Optionally, you may also add the following entries (they will be 0 if \n"
					"not given):\n"
					"\n"
					"  * dataversion\n"
					"  * maxdataversion\n"
					"  \n"
					"Note that for all times, it is neccessary to explicitly specify time zone. \n"
					"Otherwise you may experience some problems with loading.\n";

	return s;
}


namespace
{
std::string getDefaultUser()
{
	char * ret = getenv("USER");
	if ( ret )
		return ret;
	return "wdb";
}
}

boost::program_options::options_description Configuration::shownOptions_()
{
	options_description general( "General" );
    general.add_options()
	( "help", bool_switch(), "Produce help message" )
	( "version", bool_switch(), "Produce version information, then exit" )
	;
    options_description database("Database configuration");
    database.add_options()
    ( "database,d", value(& database_)->default_value("wdb"), "Database name" )
    ( "host,h", value(& host_), "Database host (ex. somehost.met.no)" )
    ( "user,u", value(& user_)->default_value( getDefaultUser() ), "Database user name" )
    ( "port,p", value<int>(& port_), "Database port number to connect to" )
    ;
    options_description logging("Logging");
    logging.add_options()
    ( "loglevel", value(& logLevel_)->default_value( 3 ), "Logging level, from 1 (most) to 5 (least)" )
    ( "logfile", value(& logFile_ ), "Name of logfile. If not set stdout will be used" )
    ;

	options_description wci( "wci" );
	wci.add_options()
	("namespace", value(& nameSpace_), "Use the given wdb namespaces, instead of what is specified in the given file(s)");

	options_description loading( "Loading" );
	loading.add_options()
	("only-groups", bool_switch(& onlyCreateCroups_), "Do not attempt to load regular data. Instead only create floatvalue groups for the data");

	boost::program_options::options_description shownOptions("Allowed options");
	shownOptions.add(general).add(database).add(logging).add(wci).add(loading);

	return shownOptions;
}

void Configuration::parseOptions_(int argc, char ** argv)
{
	boost::program_options::options_description options = shownOptions_();

	options_description input( "input" );
	input.add_options()
	("filename", value(& file_), "Read data from the given file(s)");

	options.add(input);

    positional_options_description p_input;
    p_input.add("filename", -1 );

	boost::program_options::variables_map givenOptions_;
	store( command_line_parser( argc, argv ).
				options(options).
				positional(p_input).
				run(),
			givenOptions_ );

	notify(givenOptions_);

	if ( givenOptions_["help"].as<bool>() )
	{
		printHelp(std::cout);
		exit(0);
	}
	if ( givenOptions_["version"].as<bool>() )
	{
		printVersion(std::cout);
		exit(0);
	}

	// not given value will become 0
	if ( ! givenOptions_.count("port") )
		port_ = 0;

	if ( file_.empty() )
		file_.push_back("-");
}

} /* namespace fastload */
