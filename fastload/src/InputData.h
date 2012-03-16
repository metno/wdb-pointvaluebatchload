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

#ifndef INPUTDATA_H_
#define INPUTDATA_H_

#include <string>


namespace fastload
{

class InputData
{
public:
	InputData(const std::string & inputLine, const std::string dataprovider);
	~InputData();

	const std::string & dataprovider() const { return dataprovider_; }
	float value() const { return value_; }
	const std::string & placename() const { return placename_; }
	const std::string & referencetime() const { return referencetime_; }
	const std::string & validfrom() const { return validfrom_; }
	const std::string & validto() const { return validto_; }
	const std::string & valueparametername() const { return valueparametername_; }
	const std::string & levelparametername() const { return levelparametername_; }
	float levelfrom() const { return levelfrom_; }
	float levelto() const { return levelto_; }
	unsigned dataversion() const { return dataversion_; }
	unsigned maxdataversion() const { return maxdataversion_; }


private:
	std::string dataprovider_;
	float value_;
	std::string placename_;
	std::string referencetime_;
	std::string validfrom_;
	std::string validto_;
	std::string valueparametername_;
	std::string levelparametername_;
	float levelfrom_;
	float levelto_;
	unsigned dataversion_;
	unsigned maxdataversion_;
};

} /* namespace fastload */
#endif /* INPUTDATA_H_ */
