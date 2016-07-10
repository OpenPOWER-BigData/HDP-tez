/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

App.Helpers.number = {

  MAX_SAFE_INTEGER: 9007199254740991,

  /**
   * Convert byte size to other metrics.
   * 
   * @param {Number} bytes to convert to string
   * @param {Number} precision Number to adjust precision of return value. Default is 0.
   * @param {String} parseType
   *           JS method name for parse string to number. Default is "parseInt".
   * @param {Number} multiplyBy bytes by this number if given. This is needed
   *          as <code>null * 1024 = 0</null>
   * @remarks The parseType argument can be "parseInt" or "parseFloat".
   * @return {String} Returns converted value with abbreviation.
   */
  bytesToSize: function (bytes, precision, parseType, multiplyBy) {
    if (isNaN(bytes)) bytes = 0;
    if (Em.isNone(bytes)) {
      return 'n/a';
    } else {
      if (arguments[2] === undefined) {
        parseType = 'parseInt';
      }
      if (arguments[3] === undefined) {
        multiplyBy = 1;
      }
      var value = bytes * multiplyBy;
      var sizes = [ 'Bytes', 'KB', 'MB', 'GB', 'TB', 'PB' ];
      var posttxt = 0;
      while (value >= 1024) {
        posttxt++;
        value = value / 1024;
      }
      if (value === 0) {
        precision = 0;
      }
      var parsedValue = window[parseType](value);
      return parsedValue.toFixed(precision) + " " + sizes[posttxt];
    }
  },

  /**
   * Validates if the given string or number is an integer between the
   * values of min and max (inclusive). The minimum and maximum
   * checks are ignored if their valid is NaN.
   *
   * @method validateInteger
   * @param {string|number} str - input string
   * @param {string|number} [min]
   * @param {string|number} [max]
   */
  validateInteger : function(str, min, max) {
    if (Em.isNone(str) || (str + "").trim().length < 1) {
      return Em.I18n.t('number.validate.empty');
    }
    str = (str + "").trim();
    var number = parseInt(str);
    if (isNaN(number)) {
      return Em.I18n.t('number.validate.notValidNumber');
    }
    if (str.length != (number + "").length) {
      // parseInt("1abc") returns 1 as integer
      return Em.I18n.t('number.validate.notValidNumber');
    }
    if (!isNaN(min) && number < min) {
      return Em.I18n.t('number.validate.lessThanMinumum').fmt(min);
    }
    if (!isNaN(max) && number > max) {
      return Em.I18n.t('number.validate.moreThanMaximum').fmt(max);
    }
    return null;
  },

  /**
   * Format value with US style thousands separator
   * @param {string/number} value to be formatted
   * @returns {string} Formatted string
   */
  formatNumThousands: function (value) {
    if(/^[\d\.]+$/.test(value)) {
      var parts = value.toString().split(".");
      parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      return parts.join(".");
    }
    return value;
  },

  /**
   * Checks if the value is an integer or can be converted to an integer.
   * a value of NaN returns false.
   * @method: isValidInt
   * @param {string|number} value to check
   * @return {boolean} 
   */
  isValidInt: function(value) {
    return value % 1 == 0;
  },

  /**
   * converts fraction to percentage.
   * @param {number} fraction assumes < 1
   * @return {float} fixed decimal point formatted percentage
   */
  fractionToPercentage: function(number, decimal) {
    decimal = decimal || 2;
    return parseFloat((number * 100).toFixed(decimal)) + ' %';
  }

};