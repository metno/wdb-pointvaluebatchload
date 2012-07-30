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

#include "DataQueue.h"
#include <log4cpp/Category.hh>

namespace fastload
{

DataQueue::DataQueue(unsigned maxQueSize, const std::string & queName) :
		status_(Ready),
		maxQueSize_(maxQueSize),
		queName_(queName),
		inserts_(0),
		extracts_(0)
{
}

DataQueue::~DataQueue()
{
}

bool DataQueue::get(DataQueue::Data & out)
{
//	log4cpp::Category & log = log4cpp::Category::getInstance( "wdb.fastload.DataQue.get." + queName_ );

	boost::unique_lock<boost::mutex> lock(mutex_);

	//log.debugStream() << "size: " << que_.size() << " (total: " << extracts_ << ")";

	if ( status_ == Shutdown )
		throw std::runtime_error(queName_ + " queue was terminated");

	while ( que_.empty() )
	{
		if ( status_ == Done )
			return false;

		//log.debugStream() << "get waiting";
		condition_.wait(mutex_);
		//log.debugStream() << "get done waiting";

		if ( status_ == Shutdown )
			throw std::runtime_error(queName_ + " queue was terminated");

	}

	out = que_.front();
	que_.pop_front();

	if ( maxQueSize_ and que_.size() == maxQueSize_ / 2 )
		condition_.notify_all();

	++ extracts_;

	return true;
}

void DataQueue::put(const DataQueue::Data & element)
{
	//log4cpp::Category & log = log4cpp::Category::getInstance( "wdb.fastload.DataQue.put" + queName_ );

	boost::unique_lock<boost::mutex> lock(mutex_);

	if ( status_ == Shutdown )
		throw std::runtime_error(queName_ + " queue was terminated");

	if ( maxQueSize_ and que_.size() >= maxQueSize_ )
	{
		//log.debugStream() << "waiting (" << que_.size() << ")";
		condition_.wait(mutex_);
		//log.debugStream() << "put done waiting";
	}

	que_.push_back(element);
	//log.debugStream() << "Size: " << que_.size();
	++ inserts_;
	condition_.notify_one();
}

void DataQueue::done()
{
	status_ = Done;
	condition_.notify_all();
}

void DataQueue::shutdown()
{
	log4cpp::Category & log = log4cpp::Category::getInstance( "wdb.fastload.DataQue." + queName_ );
	log.warn("shutdown");

	status_ = Shutdown;
	condition_.notify_all();
}

}
