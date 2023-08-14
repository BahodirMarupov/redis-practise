package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval.MINUTE;

@Component
public class JedisRateLimitService implements RateLimitService {

    private static final Logger logger = LoggerFactory.getLogger(JedisRateLimitService.class);

    private final static String REDIS_KEY_DELIMINATOR = ":";

    private final Set<RateLimitRule> rateLimitRules;
    private final JedisCluster jedisCluster;

    public JedisRateLimitService(Set<RateLimitRule> rateLimitRules, JedisCluster jedisCluster) {
        this.rateLimitRules = rateLimitRules;
        this.jedisCluster = jedisCluster;
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {
        return requestDescriptors.stream().anyMatch(this::shouldLimit);
    }

    private boolean shouldLimit(RequestDescriptor requestDescriptor) {
        Optional<RateLimitRule> descriptorRuleOpt = findDescriptorRule(requestDescriptor);
        if (descriptorRuleOpt.isPresent()) {
            String key = buildKey(requestDescriptor);
            RateLimitRule rateLimitRule = descriptorRuleOpt.get();
            String value = jedisCluster.get(key);
            if (value != null) {
                int count = Integer.parseInt(value);
                if (count > rateLimitRule.getAllowedNumberOfRequests()) {
                    return true;
                } else {
                    jedisCluster.setex(key, MINUTE.equals(rateLimitRule.getTimeInterval()) ? 60 : 60 * 60, String.valueOf(++count));
                }
            } else {
                jedisCluster.setex(key, MINUTE.equals(rateLimitRule.getTimeInterval()) ? 60 : 60 * 60, "1");
            }
        } else {
            logger.info("There is not rule for request descriptor: {}", requestDescriptor);
        }
        return false;
    }

    private String buildKey(RequestDescriptor descriptor) {
        List<String> descriptorFields = new ArrayList<>();
        descriptor.getAccountId().ifPresent(descriptorFields::add);
        descriptor.getClientIp().ifPresent(descriptorFields::add);
        descriptor.getRequestType().ifPresent(descriptorFields::add);
        return String.join(REDIS_KEY_DELIMINATOR, descriptorFields);
    }

    private Optional<RateLimitRule> findDescriptorRule(RequestDescriptor descriptor) {
        Optional<RateLimitRule> matchedStrictRule = rateLimitRules.stream()
                .filter(limitRule -> !isGeneralRule(limitRule))
                .filter(rateLimitRule -> isFieldsMatched(descriptor.getClientIp(), rateLimitRule.getClientIp())
                        && isFieldsMatched(descriptor.getAccountId(), rateLimitRule.getAccountId())
                        && isFieldsMatched(descriptor.getRequestType(), rateLimitRule.getRequestType()))
                .findFirst();
        Optional<RateLimitRule> generalRule = rateLimitRules.stream()
                .filter(this::isGeneralRule)
                .findFirst();
        return matchedStrictRule.isPresent() ? matchedStrictRule : generalRule;
    }

    private boolean isGeneralRule(RateLimitRule rateLimitRule) {
        return isFieldEmpty(rateLimitRule.getAccountId())
                && isFieldEmpty(rateLimitRule.getClientIp())
                && isFieldEmpty(rateLimitRule.getRequestType());
    }

    private boolean isFieldEmpty(Optional<String> field) {
        return !field.isPresent() || field.get().isEmpty();
    }

    private boolean isFieldsMatched(Optional<String> descriptorField, Optional<String> ruleField) {
        if (isFieldEmpty(ruleField)) {
            return true;
        }
        return descriptorField.map(s -> s.equals(ruleField.get())).orElse(false);
    }


}
