const debug = require('debug')('divvy');
const fs = require('fs');
const ini = require('ini');
const path = require('path');

const Utils = require('./utils');
const Constants = require('./constants');
const ZooKeeper = require('zookeeper');


/**
 * In support of globbing, we turn the operation value into
 * a regex. We don't want to support full regex keys (we may
 * in the future, however that will be an explicit decision).
 * These characters are escaped from globbed keys before being
 * parsed into a regex ensuring that we only support globs.
 * The tl;dr of it is that it represents special regex chars
 * excluding "*".
 */
const REGEX_ESCAPE_CHARACTERS = /[-[\]{}()+?.,\\^$|#]/g;

/**
 * Allowed pattern for the "label" field of a rule.
 */
const RULE_LABEL_REGEX = /^[a-zA-Z0-9_-]{1,255}$/;

function isGlobValue(v) {
  return v.match(/\*/);
}

class Config {
  constructor() {
    this.rules = [];
    this.ruleLabels = new Set();
    //setInterval(Config.get_alerts, 5000);
  }

  /**
   * Takes a glob rule value (e.g. /my/path/*) and creates a regex to
   * test the incoming operation value with.
   * @param {string} ruleValue The glob rule value to parse to regex.
   * @return {RegExp} The regex to test the operation value with.
   */
  static parseGlob(ruleValue) {
    ruleValue = ruleValue.replace(REGEX_ESCAPE_CHARACTERS, '\\$&');
    ruleValue = ruleValue.replace('*', '.*');
    return new RegExp(`^${ruleValue}`);
  }

  createClient(timeoutMs = 5000) {
    const config = {
        connect: "host.docker.internal:2199",
        timeout: timeoutMs,
        debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false,
    };

    return new ZooKeeper(config);
  }

  static fromJsonFile(filename) {
    console.log("reading config from the json file")
    const rawConfig = JSON.parse(fs.readFileSync(filename, 'utf-8'));
    const config = new Config();
    config.zookeeperClient = config.createClient()

    config.zookeeperClient.on('connect', () => {
        console.log("connected to zookeeper")
    });

    // Add default after other rules since it has lowest precedence
    if (typeof rawConfig.default === 'object') {
      rawConfig.overrides.push(rawConfig.default);
    }

    (rawConfig.overrides || []).forEach(function (rule) {
      config.addRule({
        operation: Utils.stringifyObjectValues(rule.operation),
        creditLimit: rule.creditLimit,
        resetSeconds: rule.resetSeconds,
        actorField: rule.actorField,
        matchPolicy: rule.matchPolicy,
        label: rule.label,
        comment: rule.comment,
      });
    });

    config.validate();
    return config;
  }

  static fromFile(filename) {
    switch (path.extname(filename)) {
      case '.json':
        return this.fromJsonFile(filename);
      case '.ini':
        return this.fromIniFile(filename);
      default:
        throw new Error(`Unrecognized format for config file: ${filename}`);
    }
  }

  async createNode(client) {
    const data = 'hello'
    console.log("here in createNode!!!!" +  Date.now())
    const createdPath = await client.create("/test70/worker/message/msg", data, ZooKeeper.ZOO_PERSISTENT).catch(error => console.log("wtf" + error.message));
  }

  async createPath(client, config) {
    const path = '/workers/worker';
    const data = ''
    try {
      console.log("trying to createeeeeeeeeeeee path!")
      console.log(ZooKeeper.ZOO_EPHEMERAL)
      console.log(ZooKeeper.ZOO_SEQUENCE)

      const promises = [];

      const message_func = function(error) {
          if (error != null) {
            console.log(error)
            console.log("error creating the path");
          } else {
            console.log("no error!!!!!!!! after creating path")
            //const data = 'hello'
            //const createdPath = client.create("/mandy118/worker/message/msg", data, ZooKeeper.ZOO_PERSISTENT).catch(error => console.log("wtf" + error.message));
          }
      }

      const mkdirPath = await client.mkdirp("/test70/worker/message", message_func);
      const stat = await client.exists('/test70/worker/message', false)
      //const mkdirPath = await client.create("/test70/worker/message/msg", '', ZooKeeper.ZOO_EPHEMERAL | ZooKeeper.ZOO_SEQUENCE);
      const mkNode = await config.createNode(client);

    } catch (error) {
      console.log(error)
      console.log("hello! error!")
    }
  }

  /** Creates a new instance from an `ini` file.  */
  static fromIniFile(filename) {
    console.log("reading from ini file")
    const rawConfig = ini.parse(fs.readFileSync(filename, 'utf-8'));
    const config = new Config();
    config.zookeeperClient = config.createClient();
    config.zookeeperClient.init({
        connect: "host.docker.internal:2199",
        timeout: 5000,
        debug_level: ZooKeeper.ZOO_LOG_LEVEL_DEBUG,
        host_order_deterministic: false,
    })

/*
    config.zookeeperClient.on('connect', () => {
        console.log("connected to zookeeper");
        config.createPath(config.zookeeperClient);
    });
*/
    config.createPath(config.zookeeperClient, config);/*.then(
          function(value) {
            console.log("create path successful " + Date.now());
            config.createNode(config.zookeeperClient);
          }
    );*/

    //config.createNode(config.zookeeperClient);


    for (const rulegroupString of Object.keys(rawConfig)) {
      const rulegroupConfig = rawConfig[rulegroupString];

      // These fields are required and will be validated within addRule
      const operation = Config.stringToOperation(rulegroupString);
      const creditLimit = parseInt(rulegroupConfig.creditLimit, 10);
      const resetSeconds = parseInt(rulegroupConfig.resetSeconds, 10);

      // Optional fields.
      const actorField = rulegroupConfig.actorField || '';
      const matchPolicy = rulegroupConfig.matchPolicy || '';
      const comment = rulegroupConfig.comment || '';
      const label = rulegroupConfig.label || '';

      config.addRule({
        operation, creditLimit, resetSeconds, actorField, matchPolicy, label, comment,
      });
    }

    console.log(config)
    config.validate();
    return config;
  }

  /** Converts a string like `a=b c=d` to an operation like `{a: 'b', c: 'd'}`. */
  static stringToOperation(s) {
    const operation = {};
    if (s === 'default') {
      return operation;
    }
    for (const kv of s.split(/\s+/)) {
      const pair = kv.split('=');
      operation[pair[0]] = pair[1] || '';
    }
    return operation;
  }

  get_alerts() {
    setInterval(this.get_alert, 5000);
    /*
    const config = new Config()
    const rule = {
      operation: {"method": "GET", "path": "/status"},
      creditLimit: 1000,
      resetSeconds: 60,
      actorField: null,
      matchPolicy: Constants.MATCH_POLICY_STOP,
      label: null,
      comment: null,
    };
    config.addRule(rule);
    console.log(config)
    console.log("printing message!!")
    */
  }

  get_alert() {
    const rule = {
      operation: {},
      creditLimit: 1000,
      resetSeconds: 60,
      actorField: null,
      matchPolicy: Constants.MATCH_POLICY_CANARY,
      label: null,
      comment: null,
    };
    this.rules.push(rule)
    //console.log("add successful!!")
    return rule
  }

  /**
   * Installs a new rule with least significant precendence (append).
   *
   * @param {Object} operation    The "operation" to be rate limited, specifically,
   *                              a map of free-form key-value pairs.
   * @param {number} creditLimit  Number of operations to permit every `resetSeconds`
   * @param {number} resetSeconds Credit renewal interval.
   * @param {string} actorField   Name of the actor field (optional).
   * @param {string} matchPolicy  Match policy (optional).
   * @param {string} label        Optional name for this rule.
   * @param {string} comment      Optional diagnostic name for this rule.
   */
  addRule({
    operation, creditLimit, resetSeconds, actorField, matchPolicy, label, comment,
  }) {
    if (!operation) {
      throw new Error('Operation must be specified.');
    }
    const firstFoundRule = this.findRules(operation)
      .find((rule) => rule.matchPolicy === Constants.MATCH_POLICY_STOP);

    if (firstFoundRule) {
      throw new Error(
        `Unreachable rule for operation=${operation}; masked by operation=${firstFoundRule.operation}`
      );
    }

    if (Number.isNaN(Number(creditLimit)) || creditLimit < 0) {
      throw new Error(`Invalid creditLimit for operation=${operation} (${creditLimit})`);
    }

    if (creditLimit > 0 && (Number.isNaN(Number(resetSeconds)) || resetSeconds < 1)) {
      throw new Error(`Invalid resetSeconds for operation=${operation} (${resetSeconds})`);
    }

    if (label) {
      if (!RULE_LABEL_REGEX.test(label)) {
        throw new Error(`Invalid rule label "${label}"; must match ${RULE_LABEL_REGEX}`);
      } else if (this.ruleLabels.has(label)) {
        throw new Error(`A rule with label "${label}" already exists; labels must be unique.`);
      }
      this.ruleLabels.add(label);
    }

    if (matchPolicy) {
      switch (matchPolicy) {
        case Constants.MATCH_POLICY_STOP:
        case Constants.MATCH_POLICY_CANARY:
          break;
        default:
          throw new Error(`Invalid matchPolicy "${matchPolicy}"`);
      }
    }

    const rule = {
      operation,
      creditLimit,
      resetSeconds,
      actorField: actorField || null,
      matchPolicy: matchPolicy || Constants.MATCH_POLICY_STOP,
      label: label || null,
      comment: comment || null,
    };
    this.rules.push(rule);

    debug('config: installed rule: %j', rule);
  }

  /**
   * Validate that this is a valid Config instance.
   */
  validate() {
    if (!this.rules.length) {
      throw new Error('Config does not define any rules.');
    }
    const lastRule = this.rules[this.rules.length - 1];
    console.log(Object.keys(lastRule.operation))
    if (Object.keys(lastRule.operation).length !== 0) {
      throw new Error('Config does not define a default rule.');
    }
  }

  /**
   * Finds all rules matching this operation and returns them as an array.
   *
   * In typical usage, the result will be length 1 (the request
   * matched a rule with matchPolicy "stop").
   *
   * In more advanced usages, additional "canary" rules may be returned
   * ahead of a final "stop" rule.
   */
  findRules(operation) {
    const result = [];
    //console.log("finding rules")
    for (const rule of this.rules) {
      if (!rule.matchPolicy) {
        throw new Error('Bug: Rule does not define a match policy.');
      }

      let match = true;
      console.log(Object.keys(rule.operation))
      for (const operationKey of Object.keys(rule.operation)) {
        const operationValue = rule.operation[operationKey];
        if (operationValue === '*') {
          match = true;
        } else if (isGlobValue(operationValue)) {
          match = Config.parseGlob(operationValue).test(operation[operationKey]);
        } else if (operationValue !== operation[operationKey]) {
          match = false;
        }

        // Skip testing additional operations if rule has already failed.
        if (!match) {
          break;
        }
      }

      if (match) {
        result.push(rule);
        if (rule.matchPolicy === Constants.MATCH_POLICY_STOP) {
          break;
        }
      }
    }
    result.push(this.get_alert())
    this.validate()
    //console.log(this.rules)

    return result;
  }

  toJson(pretty) {
    const data = {
      overrides: [],
    };
    for (const rule of this.rules) {
      if (Object.keys(rule.operation).length === 0) {
        data.default = rule;
      } else {
        data.overrides.push(rule);
      }
    }
    return JSON.stringify(data, null, pretty && 2);
  }
}

module.exports = Config;
