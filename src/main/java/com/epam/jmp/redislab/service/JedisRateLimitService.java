package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
public class JedisRateLimitService implements RateLimitService{

    private final List<RateLimitRule> rateLimitRules;

    public JedisRateLimitService(List<RateLimitRule> rateLimitRules) {
        this.rateLimitRules = rateLimitRules;
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {
        return false;
    }
}
