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

#include "NewStyleTranslateJob.h"
#include "DatabaseTranslator.h"
#include "InputData.h"

namespace fastload
{

NewStyleTranslateJob::NewStyleTranslateJob(DataQueue::Ptr readQueue, DataQueue::Ptr writeQueue, boost::shared_ptr<DatabaseTranslator> translator, bool forwardWrites) :
		TranslateJob(readQueue, writeQueue, translator, forwardWrites)
{
}

NewStyleTranslateJob::~NewStyleTranslateJob()
{
}

std::string NewStyleTranslateJob::getCopyQuery(const InputData & inputData)
{
	return "COPY " + getTableName_(inputData) + " (valuegroupid, referencetime, maxdataversion, confidencecode, value, valuestoretime) FROM STDIN";
}

std::string NewStyleTranslateJob::getCopyStatement(const InputData & inputData)
{
	int valueGroup = translator_->getValueGroup(
			inputData.dataprovider(),
			inputData.placename(),
			inputData.referencetime(),
			inputData.validfrom() - inputData.referencetime(),
			inputData.validto() - inputData.referencetime(),
			inputData.valueparametername(),
			inputData.levelparametername(), inputData.levelfrom(), inputData.levelto(),
			inputData.dataversion());

	char sep = '\t';
	std::ostringstream s;
	s << valueGroup <<sep<<
			inputData.referencetime() <<sep<<
			inputData.maxdataversion() <<sep<<
			0 <<sep<<
			inputData.value() <<sep<<
			translator_->now() << '\n';

	return s.str();
}

std::string NewStyleTranslateJob::getTableName_(const InputData & inputData)
{
	std::string tableName = translator_->getDataTableName(inputData.referencetime());
	if ( tableName.empty() )
		return "wdb_int.floatvalueitem";
	return tableName;
}

} /* namespace fastload */
