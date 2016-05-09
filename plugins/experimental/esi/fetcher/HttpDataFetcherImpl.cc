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

#include "HttpDataFetcherImpl.h"
#include "lib/Utils.h"
#include "lib/gzip.h"
#include "EsiGunzip.h"

#include <arpa/inet.h>
#include <stdlib.h>

using std::string;
using namespace EsiLib;

const int HttpDataFetcherImpl::FETCH_EVENT_ID_BASE = 10000;

inline void
HttpDataFetcherImpl::_release(RequestData &req_data)
{
  if (req_data.bufp && !req_data.stream_enabled) {
    if (req_data.hdr_loc) {
      TSHandleMLocRelease(req_data.bufp, TS_NULL_MLOC, req_data.hdr_loc);
      req_data.hdr_loc = 0;
    }
    TSMBufferDestroy(req_data.bufp);
    req_data.bufp = 0;
  }
  if (req_data.stream_enabled && req_data.gunzip) {
    delete req_data.gunzip;
    req_data.gunzip = 0;
  }
}

HttpDataFetcherImpl::HttpDataFetcherImpl(TSCont contp, sockaddr const *client_addr, const char *debug_tag)
  : _contp(contp), _n_pending_requests(0), _curr_event_id_base(FETCH_EVENT_ID_BASE), _headers_str(""), _client_addr(client_addr)
{
  _http_parser = TSHttpParserCreate();
  snprintf(_debug_tag, sizeof(_debug_tag), "%s", debug_tag);
}

HttpDataFetcherImpl::~HttpDataFetcherImpl()
{
  clear();
  TSHttpParserDestroy(_http_parser);
}

bool
HttpDataFetcherImpl::addFetchRequest(const string &url, bool is_stream /* = false */, FetchedDataProcessor *callback_obj /* = 0 */)
{
  // do we already have a request for this?
  std::pair<UrlToContentMap::iterator, bool> insert_result =
    _pages.insert(UrlToContentMap::value_type(url, RequestData(is_stream)));
  if (callback_obj) {
    ((insert_result.first)->second).callback_objects.push_back(callback_obj);
  }
  if (!insert_result.second) {
    TSDebug(_debug_tag, "[%s] Fetch request for url [%s] already added", __FUNCTION__, url.data());
    return true;
  }

  if (!is_stream) { // if streaming is disabled
    char buff[1024];
    char *http_req;
    int length;

    length = sizeof("GET ") - 1 + url.length() + sizeof(" HTTP/1.0\r\n") - 1 + _headers_str.length() + sizeof("\r\n") - 1;
    if (length < (int)sizeof(buff)) {
      http_req = buff;
    } else {
      http_req = (char *)malloc(length + 1);
      if (http_req == NULL) {
        TSError("[HttpDataFetcherImpl][%s] malloc %d bytes fail", __FUNCTION__, length + 1);
        return false;
      }
    }

    sprintf(http_req, "GET %s HTTP/1.0\r\n%s\r\n", url.c_str(), _headers_str.c_str());

    TSFetchEvent event_ids;
    event_ids.success_event_id = _curr_event_id_base;
    event_ids.failure_event_id = _curr_event_id_base + 1;
    event_ids.timeout_event_id = _curr_event_id_base + 2;
    _curr_event_id_base += 3;

    TSFetchUrl(http_req, length, _client_addr, _contp, AFTER_BODY, event_ids);
    if (http_req != buff) {
      free(http_req);
    }
    _page_entry_lookup.push_back(insert_result.first);
    TSDebug(_debug_tag, "[%s] Successfully added fetch request for URL [%s]", __FUNCTION__, url.data());
  } else { // if streaming is enabled
    TSFetchSM fetch_sm =
      TSFetchCreate(_contp, "GET", url.c_str(), "HTTP/1.0", _client_addr, TS_FETCH_FLAGS_STREAM | TS_FETCH_FLAGS_DECHUNK);
    for (HttpHeaderList::iterator iter = _header_list.begin(); iter != _header_list.end(); ++iter) {
      TSFetchHeaderAdd(fetch_sm, (*iter).name, (*iter).name_len, (*iter).value, (*iter).value_len);
    }
    _stream_entry_lookup.push_back(insert_result.first);
    TSFetchUserDataSet(fetch_sm, static_cast<void *>(_stream_entry_lookup.back()));
    TSFetchLaunch(fetch_sm);

    TSDebug(_debug_tag, "[%s] Successfully added stream fetch request for URL [%s]", __FUNCTION__, url.data());
  }
  ++_n_pending_requests;
  _page_entry_lookup.push_back(insert_result.first);
  return true;
}

bool
HttpDataFetcherImpl::_isFetchEvent(TSEvent event, int &base_event_id) const
{
  base_event_id = _getBaseEventId(event);
  if ((base_event_id < 0) || (base_event_id >= static_cast<int>(_page_entry_lookup.size()))) {
    TSDebug(_debug_tag, "[%s] Event id %d not within fetch event id range [%d, %ld)", __FUNCTION__, event, FETCH_EVENT_ID_BASE,
            static_cast<long int>(FETCH_EVENT_ID_BASE + (_page_entry_lookup.size() * 3)));
    return false;
  }
  return true;
}

bool
HttpDataFetcherImpl::_isStreamFetchEvent(TSEvent event) const
{
  if (event == (TSEvent)TS_FETCH_EVENT_EXT_HEAD_READY || event == (TSEvent)TS_FETCH_EVENT_EXT_HEAD_DONE ||
      event == (TSEvent)TS_FETCH_EVENT_EXT_BODY_READY || event == (TSEvent)TS_FETCH_EVENT_EXT_BODY_DONE) {
    TSDebug(_debug_tag, "[%s] Stream Event [%d]", __FUNCTION__, event);
    return true;
  }
  return false;
}

bool
HttpDataFetcherImpl::handleFetchEvent(TSEvent event, void *edata)
{
  if (_isStreamFetchEvent(event)) {
    return handleStreamFetchEvent(event, edata);
  }

  int base_event_id;
  if (!_isFetchEvent(event, base_event_id)) {
    TSError("[HttpDataFetcherImpl][%s] Event %d is not a fetch event", __FUNCTION__, event);
    return false;
  }

  UrlToContentMap::iterator &req_entry = _page_entry_lookup[base_event_id];
  const string &req_str = req_entry->first;
  RequestData &req_data = req_entry->second;

  if (req_data.complete == STATUS_DATA_AVAILABLE) {
    // can only happen if there's a bug in this or fetch API code
    TSError("[HttpDataFetcherImpl][%s] URL [%s] already completed; Retaining original data", __FUNCTION__, req_str.c_str());
    return false;
  }

  --_n_pending_requests;
  req_data.complete = STATUS_DATA_AVAILABLE;

  int event_id = (static_cast<int>(event) - FETCH_EVENT_ID_BASE) % 3;
  if (event_id != 0) { // failure or timeout
    TSError("[HttpDataFetcherImpl][%s] Received failure/timeout event id %d for request [%s]", __FUNCTION__, event_id,
            req_str.data());
    return true;
  }

  int page_data_len;
  const char *page_data = TSFetchRespGet(static_cast<TSHttpTxn>(edata), &page_data_len);
  req_data.response.assign(page_data, page_data_len);
  bool valid_data_received = false;
  const char *startptr = req_data.response.data(), *endptr = startptr + page_data_len;

  req_data.bufp = TSMBufferCreate();
  req_data.hdr_loc = TSHttpHdrCreate(req_data.bufp);
  TSHttpHdrTypeSet(req_data.bufp, req_data.hdr_loc, TS_HTTP_TYPE_RESPONSE);
  TSHttpParserClear(_http_parser);

  if (TSHttpHdrParseResp(_http_parser, req_data.bufp, req_data.hdr_loc, &startptr, endptr) == TS_PARSE_DONE) {
    req_data.resp_status = TSHttpHdrStatusGet(req_data.bufp, req_data.hdr_loc);
    valid_data_received = true;
    if (req_data.resp_status == TS_HTTP_STATUS_OK) {
      req_data.body_len = endptr - startptr;
      req_data.body = startptr;
      TSDebug(_debug_tag, "[%s] Inserted page data of size %d starting with [%.6s] for request [%s], Updated status [%d]",
              __FUNCTION__, req_data.body_len, (req_data.body_len ? req_data.body : "(null)"), req_str.c_str(), req_data.complete);

      if (_checkHeaderValue(req_data.bufp, req_data.hdr_loc, TS_MIME_FIELD_CONTENT_ENCODING, TS_MIME_LEN_CONTENT_ENCODING,
                            TS_HTTP_VALUE_GZIP, TS_HTTP_LEN_GZIP, false)) {
        BufferList buf_list;
        req_data.raw_response = "";
        if (gunzip(req_data.body, req_data.body_len, buf_list)) {
          for (BufferList::iterator iter = buf_list.begin(); iter != buf_list.end(); ++iter) {
            req_data.raw_response.append(iter->data(), iter->size());
          }
        } else {
          TSError("[HttpDataFetcherImpl][%s] Error while gunzipping data", __FUNCTION__);
        }
        req_data.body_len = req_data.raw_response.size();
        req_data.body = req_data.raw_response.data();
      }

      for (CallbackObjectList::iterator list_iter = req_data.callback_objects.begin(); list_iter != req_data.callback_objects.end();
           ++list_iter) {
        (*list_iter)->processData(req_str.data(), req_str.size(), req_data.body, req_data.body_len);
      }

    } else {
      TSDebug(_debug_tag, "[%s] Received non-OK status %d for request [%s]", __FUNCTION__, req_data.resp_status, req_str.data());

      string empty_response = "";
      for (CallbackObjectList::iterator list_iter = req_data.callback_objects.begin(); list_iter != req_data.callback_objects.end();
           ++list_iter) {
        (*list_iter)->processData(req_str.data(), req_str.size(), empty_response.data(), empty_response.size());
      }
    }
  } else {
    TSDebug(_debug_tag, "[%s] Could not parse response for request [%s]", __FUNCTION__, req_str.data());
  }

  if (!valid_data_received) {
    _release(req_data);
    req_data.response.clear();
  }

  return true;
}

bool
HttpDataFetcherImpl::handleStreamFetchEvent(TSEvent event, void *edata)
{
  bool valid_data_received = true;
  TSFetchSM fetch_sm = static_cast<TSFetchSM>(edata);
  UrlToContentMap::iterator *req_entry = static_cast<UrlToContentMap::iterator *>(TSFetchUserDataGet(fetch_sm));

  const string &req_str = (*req_entry)->first;
  RequestData &req_data = (*req_entry)->second;

  if (req_data.complete == STATUS_DATA_AVAILABLE) {
    // can only happen if there's a bug in this or fetch API code
    TSDebug("[%s] URL [%s] already completed; Retaining original data", __FUNCTION__, req_str.c_str());
    return false;
  }

  TSFetchEventExt ext_event = static_cast<TSFetchEventExt>(event);
  switch (ext_event) {
  case TS_FETCH_EVENT_EXT_HEAD_DONE: {
    req_data.bufp = TSFetchRespHdrMBufGet(fetch_sm);
    req_data.hdr_loc = TSFetchRespHdrMLocGet(fetch_sm);
    req_data.resp_status = TSHttpHdrStatusGet(req_data.bufp, req_data.hdr_loc);
    valid_data_received = true;
    if (req_data.resp_status == TS_HTTP_STATUS_OK) {
      // TSDebug(_debug_tag, "[%s] Received non-OK status %d for request [%s]",
      //__FUNCTION__, req_data.resp_status, req_str.data());
      if (_checkHeaderValue(req_data.bufp, req_data.hdr_loc, TS_MIME_FIELD_CONTENT_ENCODING, TS_MIME_LEN_CONTENT_ENCODING,
                            TS_HTTP_VALUE_GZIP, TS_HTTP_LEN_GZIP, false)) {
        req_data.stream_encoded = true;
      }
    }
    TSDebug(_debug_tag, "[%s] HEAD_DONE event received. Checking Header Value stream status [%d]", __FUNCTION__,
            req_data.stream_encoded);
  } break;
  case TS_FETCH_EVENT_EXT_BODY_READY: {
    // keep appending  to stringstream

    req_data.body_len = TSFetchReadData(fetch_sm, req_data.rsp_buffer, sizeof(req_data.rsp_buffer));
    TSDebug(_debug_tag, "[%s]  received  [%d] bytes of data starting with [%.10s]", __FUNCTION__, req_data.body_len, req_data.body);
    // req_data.rsp_stream->write(req_data.rsp_buffer, req_data.body_len);
    // req_data.rsp_stream.write(req_data.rsp_buffer, req_data.body_len);
    req_data.body = req_data.rsp_buffer;
    req_data.complete = STATUS_DATA_PARTIAL;
    TSDebug(_debug_tag, "[%s]  appending data of length [%d]. status %d", __FUNCTION__, (int)req_data.body_len, req_data.complete);
    // TODO: stream gzip later.
    if (req_data.resp_status == TS_HTTP_STATUS_OK) {
      if (req_data.stream_encoded) {
        string udata;
        req_data.raw_response = "";
        if (req_data.body_len && req_data.gunzip->stream_decode(req_data.body, req_data.body_len, udata)) {
          TSDebug(_debug_tag, "[%s]  gunzipping data of length %lu", __FUNCTION__, udata.size());
          req_data.raw_response.append(udata);
        } else {
          TSError("[%s] Error while gunzipping data", __FUNCTION__);
        }
        req_data.body_len = req_data.raw_response.size();
        req_data.body = req_data.raw_response.data();
      }
      for (CallbackObjectList::iterator list_iter = req_data.callback_objects.begin(); list_iter != req_data.callback_objects.end();
           ++list_iter) {
        (*list_iter)->processData(req_str.data(), req_str.size(), req_data.body, req_data.body_len);
      }
    } else {
      TSDebug(_debug_tag, "[%s] Received non-OK status %d for request [%s]", __FUNCTION__, req_data.resp_status, req_str.data());

      string empty_response = "";
      for (CallbackObjectList::iterator list_iter = req_data.callback_objects.begin(); list_iter != req_data.callback_objects.end();
           ++list_iter) {
        (*list_iter)->processData(req_str.data(), req_str.size(), empty_response.data(), empty_response.size());
      }
    }

  } break;
  case TS_FETCH_EVENT_EXT_BODY_DONE: { // TODO: gunzip streaming

    req_data.body_len = TSFetchReadData(fetch_sm, req_data.rsp_buffer, sizeof(req_data.rsp_buffer));
    req_data.body = req_data.rsp_buffer;
    TSDebug(_debug_tag, "[%s] data received %d", __FUNCTION__, req_data.body_len);
    if (!req_data.body_len) {
      req_data.complete = STATUS_DATA_AVAILABLE;
      TSDebug(_debug_tag, "[%s] done reading data. Destroying FetchSM", __FUNCTION__);
      TSFetchDestroy(fetch_sm);
      --_n_pending_requests;
      break;
    }
    req_data.complete = STATUS_DATA_PARTIAL;
    // req_data.rsp_stream->write(req_data.rsp_buffer, req_data.body_len);
    // req_data.rsp_stream.write(req_data.rsp_buffer, req_data.body_len);
    if (req_data.resp_status == TS_HTTP_STATUS_OK) {
      if (req_data.stream_encoded) {
        req_data.raw_response = "";
        string udata;
        // TODO: check if the copy to string can be avoided.
        // string response = req_data.rsp_stream->str();
        // string response = req_data.rsp_stream.str();
        if (req_data.body_len && req_data.gunzip->stream_decode(req_data.body, req_data.body_len, udata)) {
          req_data.raw_response.append(udata);
          TSDebug(_debug_tag, "[%s] done gunzipping data of length %lu", __FUNCTION__, udata.size());
        } else {
          TSError("[%s] Error while gunzipping data", __FUNCTION__);
        }
        req_data.body_len = req_data.raw_response.size();
        req_data.body = req_data.raw_response.data();
        req_data.gunzip->stream_finish();
      }

      for (CallbackObjectList::iterator list_iter = req_data.callback_objects.begin(); list_iter != req_data.callback_objects.end();
           ++list_iter) {
        (*list_iter)->processData(req_str.data(), req_str.size(), req_data.body, req_data.body_len);
      }
    } else {
      TSDebug(_debug_tag, "[%s] Received non-OK status %d for request [%s]", __FUNCTION__, req_data.resp_status, req_str.data());

      string empty_response = "";
      for (CallbackObjectList::iterator list_iter = req_data.callback_objects.begin(); list_iter != req_data.callback_objects.end();
           ++list_iter) {
        (*list_iter)->processData(req_str.data(), req_str.size(), empty_response.data(), empty_response.size());
      }
    }
  } break;
  default:
    TSDebug(_debug_tag, "[%s] Unknown event received while handling streaming [%d]", __FUNCTION__, event);
  }
  if (!valid_data_received) {
    _release(req_data);
    req_data.response.clear();
  }
  return true;
}

bool
HttpDataFetcherImpl::_checkHeaderValue(TSMBuffer bufp, TSMLoc hdr_loc, const char *name, int name_len, const char *exp_value,
                                       int exp_value_len, bool prefix) const
{
  TSMLoc field_loc = TSMimeHdrFieldFind(bufp, hdr_loc, name, name_len);
  if (!field_loc) {
    return false;
  }

  bool retval = false;

  if (exp_value && exp_value_len) {
    const char *value;
    int value_len;
    int n_values = TSMimeHdrFieldValuesCount(bufp, hdr_loc, field_loc);

    for (int i = 0; i < n_values; ++i) {
      value = TSMimeHdrFieldValueStringGet(bufp, hdr_loc, field_loc, i, &value_len);
      if (NULL != value && value_len) {
        if (prefix) {
          if ((value_len >= exp_value_len) && (strncasecmp(value, exp_value, exp_value_len) == 0)) {
            retval = true;
          }
        } else if (Utils::areEqual(value, value_len, exp_value, exp_value_len)) {
          retval = true;
        }
      } else {
        TSDebug(_debug_tag, "[%s] Error while getting value # %d of header [%.*s]", __FUNCTION__, i, name_len, name);
      }
      if (retval) {
        break;
      }
    }
  } else { // only presence required
    retval = true;
  }
  TSHandleMLocRelease(bufp, hdr_loc, field_loc);
  return retval;
}

DataStatus
HttpDataFetcherImpl::getData(const string &url, ResponseData &resp_data) const
{
  UrlToContentMap::const_iterator iter = _pages.find(url);

  if (iter == _pages.end()) {
    TSError("[HttpDataFetcherImpl]Content being requested for unregistered URL [%s]", url.data());
    return STATUS_ERROR;
  }
  const RequestData &req_data = iter->second; // handy reference
  if (req_data.complete == STATUS_DATA_PENDING) {
    // request not completed yet
    TSError("[HttpDataFetcherImpl]Request for URL [%s] not complete", url.data());
    return STATUS_ERROR;
  } else if (req_data.complete == STATUS_DATA_PARTIAL) {
    // req_data.rsp_stream->read(body, BODY_BUFFER_SIZE);
    // size_t len = req_data.rsp_stream->gcount();
    if (req_data.body_len > 0) {
      resp_data.set(req_data.body, req_data.body_len, req_data.bufp, req_data.hdr_loc, req_data.resp_status);
      TSDebug(_debug_tag, "[%s] Found  partial data for URL [%s] of size %d starting with [%.5s]", __FUNCTION__, url.data(),
              req_data.body_len, req_data.body);
    }
  } else if (req_data.complete == STATUS_DATA_AVAILABLE) {
    if (req_data.body_len > 0) {
      resp_data.set(req_data.body, req_data.body_len, req_data.bufp, req_data.hdr_loc, req_data.resp_status);
      TSDebug(_debug_tag, "[%s] Found  data for URL [%s] of size %d starting with [%.5s]", __FUNCTION__, url.data(),
              req_data.body_len, req_data.body);
    }
  } else {
    if (req_data.response.empty()) {
      // did not receive valid data
      TSError("[HttpDataFetcherImpl]No valid data received for URL [%s]; returning empty data to be safe", url.data());
      resp_data.clear();
      return STATUS_ERROR;
    }
  }
  return req_data.complete;
}

void
HttpDataFetcherImpl::clear()
{
  for (UrlToContentMap::iterator iter = _pages.begin(); iter != _pages.end(); ++iter) {
    _release(iter->second);
  }
  _n_pending_requests = 0;
  _pages.clear();
  _page_entry_lookup.clear();
  _headers_str.clear();
  _curr_event_id_base = FETCH_EVENT_ID_BASE;
  _stream_entry_lookup.clear();
}

DataStatus
HttpDataFetcherImpl::getRequestStatus(const string &url) const
{
  UrlToContentMap::const_iterator iter = _pages.find(url);
  if (iter == _pages.end()) {
    TSError("[HttpDataFetcherImpl]Status being requested for unregistered URL [%s]", url.data());
    return STATUS_ERROR;
  }

  if ((iter->second).complete == STATUS_DATA_PENDING) {
    return STATUS_DATA_PENDING;
  }

  return (iter->second.resp_status != TS_HTTP_STATUS_OK) ? STATUS_ERROR : iter->second.complete;
}

void
HttpDataFetcherImpl::useHeader(const HttpHeader &header)
{
  // request data body would not be passed to async request and so we should not pass on the content length
  if (Utils::areEqual(header.name, header.name_len, TS_MIME_FIELD_CONTENT_LENGTH, TS_MIME_LEN_CONTENT_LENGTH)) {
    return;
  }

  // should not support partial request for async request
  if (Utils::areEqual(header.name, header.name_len, TS_MIME_FIELD_RANGE, TS_MIME_LEN_RANGE)) {
    return;
  }

  // should not support keep-alive for async requests
  if (Utils::areEqual(header.name, header.name_len, TS_MIME_FIELD_CONNECTION, TS_MIME_LEN_CONNECTION)) {
    return;
  }

  // should not support keep-alive for async requests
  if (Utils::areEqual(header.name, header.name_len, TS_MIME_FIELD_PROXY_CONNECTION, TS_MIME_LEN_PROXY_CONNECTION)) {
    return;
  }

  _headers_str.append(header.name, header.name_len);
  _headers_str.append(": ");
  _headers_str.append(header.value, header.value_len);
  _headers_str.append("\r\n");

  _header_list.push_back(header);
}

void
HttpDataFetcherImpl::useHeaders(const HttpHeaderList &headers)
{
  for (HttpHeaderList::const_iterator iter = headers.begin(); iter != headers.end(); ++iter) {
    useHeader(*iter);
  }
}
