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

#include "DataQue.h"
#include <iostream>

namespace fastload
{

DataQue::DataQue(unsigned maxQueSize, bool trackOperations) :
		done_(false),
		maxQueSize_(maxQueSize),
		trackOperations_(trackOperations),
		inserts_(0),
		extracts_(0)
{
}

DataQue::~DataQue()
{
}

bool DataQue::get(DataQue::Data & out)
{
	boost::unique_lock<boost::mutex> lock(mutex_);

	if ( trackOperations_ )
		std::cout << "get size: " << que_.size() << " (total: " << extracts_ << ")" << std::endl;

	while ( que_.empty() )
	{
		if ( done_ )
			return false;
		if ( trackOperations_ )
			std::cout << "get waiting" << std::endl;

		condition_.wait(mutex_);

		if ( trackOperations_ )
			std::cout << "get done waiting" << std::endl;
	}

	out = que_.front();
	que_.pop_front();

	if ( maxQueSize_ and que_.size() == maxQueSize_ / 2 )
		condition_.notify_all();

	++ extracts_;

	return true;
}

void DataQue::put(const DataQue::Data & element)
{
	boost::unique_lock<boost::mutex> lock(mutex_);

	if ( maxQueSize_ and que_.size() >= maxQueSize_ )
	{
		if ( trackOperations_ )
			std::cout << "put waiting (" << que_.size() << ")" << std::endl;

		condition_.wait(mutex_);

		if ( trackOperations_ )
			std::cout << "put done waiting" << std::endl;
	}

	que_.push_back(element);

	if ( trackOperations_ )
		std::cout << "put size: " << que_.size() << std::endl;

	++ inserts_;

	condition_.notify_one();
}

void DataQue::done()
{
	done_ = true;
	condition_.notify_all();
}

}
