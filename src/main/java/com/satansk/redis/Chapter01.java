package com.satansk.redis;

import com.sun.codemodel.internal.util.JavadocEscapeWriter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * Author: Kyle Song
 * Date:   下午8:58 at 18/1/28
 * Email:  satansk@hotmail.com
 */
public class Chapter01 {
    private static final int ONE_WEEK_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    /**
     * 1. hgetAll(k)
     *   （1）获取 k 对应的 hash map set 中所有 k -> v 映射
     *   （2）返回值为 Map<String, String> 类型
     */
    private void run() {
        Jedis conn = new Jedis(Constants.ip, Constants.port);

        conn.select(15);

        // 发布文章
        String articleId = postArticle(conn, "songkun", "A title", "http://www.baidu.com");

        System.out.println("Post a new article with id = " + articleId);
        System.out.println("It's HASH looks like:");

        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println(" " + entry.getKey() + ": " + entry.getValue());
        }

        // 投票
        System.out.println("-------------------------> vote");

        articleVote(conn, "name1", "article:" + articleId);
        articleVote(conn, "name2", "article:" + articleId);
        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        // 群组
        System.out.println("-------------------------> group");

        addGroups(conn, articleId, new String[] { "computer" });
        articles = getGroupArticles(conn, "computer", 10);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * 发布文章
     *
     * 1. 计数器
     *    （1）incr 对指定 key 对应的 value +1，并返回新值
     *    （2）若该 key 不存在，或其 value 并非数值，则将该 key 的 value 设置为 0，然后执行 +1
     *
     * 2. 过期时间
     *    （1）可为任意 key 设置过期时间，具备过期时间的 key 被称为 volatile key
     *    （2）为 volatile key 设置新值，或者停止 Redis 服务器都（因为 expire time 存储在硬盘上）不会影响过期时间
     *    （3）expire(k， time) 后，time 秒后 k 将过期，但 Redis 中存储的是 k 秒后那个时间点的 UNIX 时间，所以停机 Redis 不会影响过期时间
     *
     * 3. sadd(k, v)
     *    （1）将 v 添加到 k 对应的 set 中
     *    （2）若 v 在集合中已经存在，则不做任何操作
     *    （3）若 k 对应的 value 不是 set 类型，则报错
     *    （4）若 k 不存在，则创建为其创建一个 set，其中只有一个元素 v
     *
     * 4. hmset(k, Map<String, String>)
     *   （1）创建 k -> Map<String, String> 映射
     *   （2）若 k 已经存在，则使用新值覆盖旧值；若不存在，则创建
     *
     * 5. zadd(k, score, member)
     *   （1）将 member -> score 映射加入 k 对应的 sorted set 中
     *   （2）若 member 已存在，则更新其 value 为 score，并重新插入映射，以保持有序
     */
    private String postArticle(Jedis conn, String user, String title, String link) {
        // 1. 通过 article: 计数器获取文章 ID
        String articleId = String.valueOf(conn.incr("article:"));

        // 2. 为新增文章创建 voted:xxxx set，保存为其投过票的用户
        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_SECONDS);

        // 3. 为新增文章创建 article:xxxx hash map set，保存文章属性
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String, String> articleData = new HashMap<String, String>();

        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");

        conn.hmset(article, articleData);  // hash map set

        // 4. 将新增文章添加到 zset
        conn.zadd("score:", now + VOTE_SCORE, article);  // zset
        conn.zadd("time:", now, article);  // zset

        return articleId;
    }

    /**
     * 投票
     *
     * 1. sadd(k, v)
     *   （1）若 v 已经在 k 对应的 set 中，则返回 0；若不在，返回 1
     *
     * 2. zincrby(k, score, member)
     *   （1）将 member 对应的分值 + score
     *
     * 3. hincrby(k, field, value)
     *   （1）将 field 对应的值 + value
     */
    private void articleVote(Jedis conn, String user, String article) {
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_SECONDS;
        if (conn.zscore("time:", article) < cutoff) return;

        // article 格式：article:xxxx
        String articleId = article.substring(article.indexOf(":") + 1);

        /*
         * sadd: 1 成功插入新值，0 若 user 已经存在，因为若 sadd 返回 1 则表明该用户是第一次为该文章投票
         */
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes", 1L);
        }
    }

    private List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    /**
     * 获取 评分最高 or 最新发布 的文章
     *
     * 1. zrevrange
     *   （1）sorted set 的元素以 score 从高到低 顺序排列
     *   （2）取出起始 index 范围内的元素
     *   （3）除了元素排序外，与 zrange 完全相同
     *
     * 2. hgetall
     *   （1）获取 key 对应的所有 k -> v
     *   （2）返回值类型：Map<String, String>
     */
    private List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        // times: 新 -> 旧    score: 高 -> 低
        Set<String> ids = conn.zrevrange(order, start, end);

        /*
         * articles 每个元素为一个 article Map，map 中存放 article 基本信息 + id -> xx
         */
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();

        for (String id : ids) {
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);

            articles.add(articleData);
        }

        return articles;
    }

    /**
     * 添加文章到群组
     *
     * -------group:computer------ Set ----
     * :  article:1
     * :  article:2
     * :  article:3
     * :  ...
     * :  article:9287
     * ------------------------------------
     */
    private void addGroups(Jedis conn, String articleId, String[] toAdd) {
        String article = "article:" + articleId;

        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    // 获取群组文章，按发布时间排序
    private List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    /**
     * 获取群组文章
     *
     * 1. zinterscore(dstkey, String ... sets)
     *   （1）对可变参数 sets 代表的集合做交际，将结果存放在 dstKey 上
     *   （2）默认聚合方式为 ZParams.Aggregate.SUM，即将同时存在于所有集合中的 k 的分值求和，作为新 Sorted Set 的 value
     */
    private List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        // score:computer or time:computer
        String key = order + group;

        if (! conn.exists(key)) {
            // 聚合函数：MAX
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);

            // zinterscore 效率较低，将其计算结果缓存 60s
            conn.expire(key, 60);
        }

        // 从 score:computer or time:computer 中读取文章
        return getArticles(conn, page, key);
    }

    private void printArticles(List<Map<String,String>> articles) {
        for (Map<String,String> article : articles) {
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String,String> entry : article.entrySet()) {
                if (entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }

    public static void main(String[] args) {
        new Chapter01().run();
    }

}

