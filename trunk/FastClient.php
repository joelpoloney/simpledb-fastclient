<?php

/**
 * FastClient.php
 *
 * Implements a parallelized version of the Amazon SimpleDB client. 
 * This client utilizes cURL and the curl_multi* library to send
 * the requests.
 * 
 * This class requires changes to the original Amazon_SimpleDB_Client
 * class. All of the private classes will need to be made protected.
 * This is so that the FastClient can inherit them from the original
 * Client.
 *
 * This file should be placed in the same directory as its parent
 * class, Client.php.
 *
 * Copyright (c) 2008 Joel Poloney
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

require_once ("Amazon/SimpleDB/Client.php");

class Amazon_SimpleDB_Fast_Client extends Amazon_SimpleDB_Client {
		
	/**
	 * This will perform a parallelized request to get all of the objects.
	 * 
	 * @param Array		$actions	This is an array of Amazon_SimpleDB_Model_GetAttributes objects.
	 * 
	 * @return Array	This is an array of Amazon_SimpleDB_Model_GetAttributesResponse objects.
	 */
	public function getAttributes($actions) {
		if (!is_array($actions)) {
			throw new Exception ("Parameter supplied to " . __FUNCTION__ . " is not an array.");
		}
		require_once ('Amazon/SimpleDB/Model/GetAttributesResponse.php');
		
		$parameters = array();
		foreach ($actions as $action) {
			$parameters[] = $action->toMap();
		}
		
		$results = $this->_invoke($parameters);
		$objects = array();
		foreach ($results as $result) {
			if($result != "") {
				$objects[] = Amazon_SimpleDB_Model_GetAttributesResponse::fromXML($result);
			}
		}
		
		return $objects;
	}
	
	
	/**
	 * This will perform a parallelized request to put all of the objects.
	 * 
	 * @param Array		$actions	This is an array of Amazon_SimpleDB_Model_PutAttributes objects.
	 * 
	 * @return Array	This is an array of Amazon_SimpleDB_Model_PutAttributesResponse objects.
	 */
	public function putAttributes($actions) {
		if (!is_array($actions)) {
			throw new Exception ("Parameter supplied to " . __FUNCTION__ . " is not an array.");
		}
		require_once ('Amazon/SimpleDB/Model/PutAttributesResponse.php');
		
		$parameters = array();
		foreach ($actions as $action) {
			$parameters[] = $action->toMap();
		}
		
		$results = $this->_invoke($parameters);
		$objects = array();
		foreach ($results as $result) {
			if($result != "") {
				$objects[] = Amazon_SimpleDB_Model_PutAttributesResponse::fromXML($result);
			}
		}
		
		return $objects;
	}
	
	
	/**
	 * This will perform a parallelized request to delete all of the objects.
	 * 
	 * @param Array		$actions	This is an array of Amazon_SimpleDB_Model_DeleteAttributes objects.
	 * 
	 * @return Array	This is an array of Amazon_SimpleDB_Model_DeleteAttributesResponse objects.
	 */
	public function deleteAttributes($actions) {
		if (!is_array($actions)) {
			throw new Exception ("Parameter supplied to " . __FUNCTION__ . " is not an array.");
		}
		
		require_once ('Amazon/SimpleDB/Model/DeleteAttributesResponse.php');
		
		$parameters = array();
		foreach ($actions as $action) {
			$parameters[] = $action->toMap();
		}
		
		$results = $this->_invoke($parameters);
		$objects = array();
		foreach ($results as $result) {
			if($result != "") {
				$objects[] = Amazon_SimpleDB_Model_DeleteAttributesResponse::fromXML($result);
			}
		}
		
		return $objects;
    }
	
	
	/** Invoke request and return response. */
	protected function _invoke(array $parameters) {
		$actionName = $parameters["Action"];
		$responses = array();
		$responseBodies = array();

		// submit the request and read response body
		try {
			// add required request parameters
			$parameters = $this->_addRequiredParameters($parameters);

			$retries = 0;
			
			// submit the requests
			$responses = $this->_httpPost($parameters);
			
			// loop to make sure we received successful response codes (retry if necessary)
			foreach ($responses as $key => &$response) {
				do {
					try {
						if ($response['Status'] === 200) {
							$shouldRetry = FALSE;
							$responseBodies[] = $response['ResponseBody'];
							$retries = 0;
						} else {
							if ($response['Status'] === 500 || $response['Status'] === 503) {
								$shouldRetry = TRUE;
								$this->_pauseOnRetry(++$retries, $response['Status']);
								
								// retry with the parent after the pause
								$response = parent::_httpPost($parameters[$key]);
							} else {
								throw $this->_reportAnyErrors($response['ResponseBody'], $response['Status']);
							}
					   }
					} catch (Exception $e) {
						require_once ('Amazon/SimpleDB/Exception.php');
						if ($e instanceof Amazon_SimpleDB_Exception) {
							throw $e;
						} else {
							require_once ('Amazon/SimpleDB/Exception.php');
							throw new Amazon_SimpleDB_Exception(array('Exception' => $e, 'Message' => $e->getMessage()));
						}
					}
	
				} while ($shouldRetry);
			}
		} catch (Amazon_SimpleDB_Exception $se) {
			throw $se;
		} catch (Exception $t) {
			throw new Amazon_SimpleDB_Exception(array('Exception' => $t, 'Message' => $t->getMessage()));
		}

		return $responseBodies;
	}
	
	
	/** Perform HTTP post with exponential retries on error 500 and 503. */
	protected function _httpPost(array $parameters) {
		$curly = array(); // array of curl handles
		$result = array(); // data to be returned
		$multiHandle = curl_multi_init(); // multi handle
		$url = parse_url ($this->_config['ServiceURL']);
				
		foreach ($parameters as $key => $data) {
			$curly[$key] = curl_init($url['scheme'] . "://" . $url['host']);
			
			$data = $this->_getParametersAsString($data);
			
			curl_setopt($curly[$key], CURLOPT_POST, 1);
			curl_setopt($curly[$key], CURLOPT_POSTFIELDS, $data);
			curl_setopt($curly[$key], CURLOPT_RETURNTRANSFER, 1);
			curl_setopt($curly[$key], CURLOPT_HEADER, 1);
			//curl_setopt($curly[$key], CURLOPT_CONNECTTIMEOUT, 5);
			//curl_setopt($curly[$key], CURLOPT_TIMEOUT, 10);
			
			curl_multi_add_handle($multiHandle, $curly[$key]);
		}
		
		$running = null;
		do {
			curl_multi_exec($multiHandle, $running);
		} while($running > 0);
		
		foreach($curly as $key => $content) {
			$result[$key] = curl_multi_getcontent($content);
			curl_multi_remove_handle($multiHandle, $content);
		}
		
		curl_multi_close($multiHandle);
		
		foreach($result as &$response) {
			list($other, $responseBody) = explode("\r\n\r\n", $response, 2);
			$other = preg_split("/\r\n|\n|\r/", $other);
			list($protocol, $code, $text) = explode(' ', trim(array_shift($other)), 3);
			
			$response = array ('Status' => (int)$code, 'ResponseBody' => $responseBody);
		}
		
		return $result;
	}
	
	
	/** Add authentication related and version parameters */
	protected function _addRequiredParameters(array $parameters) {
		foreach ($parameters as &$parameter) {
			$parameter['AWSAccessKeyId'] = $this->_awsAccessKeyId;
			$parameter['Timestamp'] = $this->_getFormattedTimestamp();
			$parameter['Version'] = self::SERVICE_VERSION;      
			$parameter['SignatureVersion'] = $this->_config['SignatureVersion'];
			$parameter['Signature'] = $this->_signParameters($parameter, $this->_awsSecretAccessKey);
		} 
		
		return $parameters;
	}
}

?>
