/** @file

  A brief file description

  @section license License

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

#ifndef _HTTP_DATA_FETCHER_IMPL_H
#define _HTTP_DATA_FETCHER_IMPL_H

#include <string>
#include <list>
#include <vector>

#include "ts/ts.h"
#include "ts/experimental.h"
#include "lib/StringHash.h"
#include "lib/HttpHeader.h"
#include "HttpDataFetcher.h"
#include "EsiGunzip.h"

#define BODY_BUFFER_SIZE 32 * 1024
#define DEBUG_GUNZIP "plugin_esi_gunzip"

class HttpDataFetcherImpl : public HttpDataFetcher
{
public:
  HttpDataFetcherImpl(TSCont contp, sockaddr const *client_addr, const char *debug_tag);

  void useHeader(const EsiLib::HttpHeader &header);

  void useHeaders(const EsiLib::HttpHeaderList &headers);

  bool addFetchRequest(const std::string &url, FetchedDataProcessor *callback_obj = 0);

  bool handleFetchEvent(TSEvent event, void *edata);
  bool handleStreamFetchEvent(TSEvent event, void *edata);

  bool
  isFetchEvent(TSEvent event) const
  {
    int base_event_id;
    return (_isStreamFetchEvent(event) || _isFetchEvent(event, base_event_id));
  }

  bool
  isFetchComplete() const
  {
    return (_n_pending_requests == 0);
  };

  DataStatus getRequestStatus(const std::string &url) const;

  int
  getNumPendingRequests() const
  {
    return _n_pending_requests;
  };

  // used to return data to callers
  struct ResponseData {
    const char *content;
    int content_len;
    TSMBuffer bufp;
    TSMLoc hdr_loc;
    TSHttpStatus status;
    bool stream_enabled;
    ResponseData() { set(0, 0, 0, 0, TS_HTTP_STATUS_NONE); }
    inline void set(const char *c, int clen, TSMBuffer b, TSMLoc loc, TSHttpStatus s);
    void
    clear()
    {
      set(0, 0, 0, 0, TS_HTTP_STATUS_NONE);
    }
  };

  DataStatus getData(const std::string &url, ResponseData &resp_data) const;

  DataStatus
  getContent(const std::string &url, const char *&content, int &content_len) const
  {
    ResponseData resp;
    DataStatus status = getData(url, resp);
    if (status != STATUS_ERROR || status != STATUS_DATA_PENDING) {
      content = resp.content;
      content_len = resp.content_len;
    }
    return status;
  }

  void clear();

  ~HttpDataFetcherImpl();

private:
  TSCont _contp;
  char _debug_tag[64];

  typedef std::list<FetchedDataProcessor *> CallbackObjectList;

  // used to track a request that was made
  struct RequestData {
    std::string response;
    std::string raw_response;
    const char *body;
    int body_len;
    TSHttpStatus resp_status;
    CallbackObjectList callback_objects;
    DataStatus complete;
    TSMBuffer bufp;
    TSMLoc hdr_loc;
    bool stream_enabled;
    bool stream_encoded;
    EsiGunzip *gunzip;
    char rsp_buffer[BODY_BUFFER_SIZE];

    RequestData(bool stream)
      : body(0), body_len(0), resp_status(TS_HTTP_STATUS_NONE), complete(STATUS_DATA_PENDING), bufp(0), hdr_loc(0),
        stream_enabled(stream), stream_encoded(false)
    {
      gunzip = new EsiGunzip(DEBUG_GUNZIP, &TSDebug, &TSError);
    }
  };

  typedef __gnu_cxx::hash_map<std::string, RequestData, EsiLib::StringHasher> UrlToContentMap;
  UrlToContentMap _pages;

  typedef std::vector<UrlToContentMap::iterator> IteratorArray;
  IteratorArray _page_entry_lookup; // used to map event ids to requests
  IteratorArray _stream_entry_lookup;

  int _n_pending_requests;
  int _curr_event_id_base;
  TSHttpParser _http_parser;

  static const int FETCH_EVENT_ID_BASE;

  int
  _getBaseEventId(TSEvent event) const
  {
    return (static_cast<int>(event) - FETCH_EVENT_ID_BASE) / 3; // integer division
  }

  bool _isFetchEvent(TSEvent event, int &base_event_id) const;
  bool _isStreamFetchEvent(TSEvent event) const;
  bool _checkHeaderValue(TSMBuffer bufp, TSMLoc hdr_loc, const char *name, int name_len, const char *exp_value, int exp_value_len,
                         bool prefix) const;


  std::string _headers_str;
  EsiLib::HttpHeaderList _header_list;

  inline void _release(RequestData &req_data);

  sockaddr const *_client_addr;
};

inline void
HttpDataFetcherImpl::ResponseData::set(const char *c, int clen, TSMBuffer b, TSMLoc loc, TSHttpStatus s)
{
  content = c;
  content_len = clen;
  bufp = b;
  hdr_loc = loc;
  status = s;
}

#endif
