source("~/Documents/DataAnalysis/PM case/20200902撈取各縣市暑假作業數據/Scripts/func.R")


# 取得系列 missions ----
owner_account <- "assigner"
series.mission.names <- c("三年級暑假作業",
                          "四年級暑假作業",
                          "五年級暑假作業",
                          "六年級暑假作業",
                          "七年級暑假作業_國文",
                          "七年級暑假作業_英文",
                          "七年級暑假作業_數學",
                          "七年級暑假作業_生物",
                          "七年級暑假作業_地理",
                          "七年級暑假作業_歷史",
                          "七年級暑假作業_公民",
                          "八年級暑假作業_國文",
                          "八年級暑假作業_英文",
                          "八年級暑假作業_數學",
                          "八年級暑假作業_理化",
                          "八年級暑假作業_地理",
                          "八年級暑假作業_歷史",
                          "八年級暑假作業_公民",
                          "九年級暑假作業_國文",
                          "九年級暑假作業_英文",
                          "九年級暑假作業_數學",
                          "九年級暑假作業_理化",
                          "九年級暑假作業_地理",
                          "九年級暑假作業_歷史",
                          "九年級暑假作業_公民")
end_date <- "2020-09-13"
series.mission.data <-
  rbindlist(lapply(
    X = series.mission.names,
    FUN = function(series_mission_name)
      QuerySeriesMission(usersconn = conn.list$usersconn,
                         series_mission_name = series_mission_name,
                         owner_account = owner_account,
                         end_date = end_date)))
# 得取系列任務對應的年級
series.mission.data$belonging_grade <- sapply(gsub(".*(.)年級.*", "\\1", series.mission.data$series_mission_name), 
                                              function(grade_represented_in_chinese_word) {
                                                switch(grade_represented_in_chinese_word,
                                                       "一" = 1,
                                                       "二" = 2,
                                                       "三" = 3,
                                                       "四" = 4,
                                                       "五" = 5,
                                                       "六" = 6,
                                                       "七" = 7,
                                                       "八" = 8,
                                                       "九" = 9,
                                                       NA) })
# 取得系列 missions 的 questions
series.mission.questions.data <-
  rbindlist(lapply(
    X = series.mission.data$id, 
    FUN = function(mission_id) QueryMissionQuestionByMid(conn = conn.list$usersconn, 
                                                         mid = mission_id)))


# 取得跨領域 missions ----
cross_domain.mission.names <- c("[暑假作業]Line媒體素養小學堂",
                              "[暑假作業]玉山理財小達人",
                              "[暑假作業]資安小尖兵1",
                              "[暑假作業]資安小尖兵2",
                              "[暑假作業]遠傳3C抗癮小勇士",
                              "[暑假作業]遠傳3C抗癮小勇士1",
                              "[暑假作業]廣達藝術小畫家-「向大師挖寶-米勒特展」",
                              "[暑假作業]廣達藝術小畫家-富春山居圖")
mission.tbl <- tbl(conn.list$usersconn, "missions")
cross_domain.mission.data <- mission.tbl %>%
  filter(name %in% cross_domain.mission.names) %>%
  select(id, name) %>%
  collect()
# 取得跨領域 missions 的 questions
cross_domain.mission.questions.data <- 
  rbindlist(lapply(X = cross_domain.mission.data$id, 
                   FUN = function(mission_id) QueryMissionQuestionByMid(conn = conn.list$usersconn, 
                                                                        mid = mission_id)))


# 取得 user 的資訊，包含 gamecharacters, school 以及 identities (OpenID) ----
users.tbl <- tbl(conn.list$usersconn, "users")
gc.tbl <- tbl(conn.list$usersconn, "gamecharacters")
course_roles.tbl <- tbl(conn.list$usersconn, "course_roles")
gc_course_roles.tbl <- tbl(conn.list$usersconn, "gamecharacters_course_roles")
schools.tbl <- tbl(conn.list$usersconn, "schools")
identities.tbl <- tbl(conn.list$usersconn, "identities")
users.data <- users.tbl %>%
  select(id, account, email, realname, grade, school_id) %>% 
  rename(user_id = id) %>% 
  left_join(identities.tbl %>% 
              select(user_id, provider),
            by = "user_id") %>% 
  filter(provider %in% c("one_campus", "tw_edu", "ilc")) %>% 
  inner_join(schools.tbl %>% 
               select(id, name, city, code, stage) %>%
               rename(school_id = id), 
             by = "school_id") %>% 
  inner_join(gc.tbl %>%
               select(id, user_id) %>%
               rename(gamecharacter_id = id), 
             by = "user_id") %>% 
  left_join(gc_course_roles.tbl %>%    # 撈取 gc 於課程中的角色
              inner_join(course_roles.tbl, 
                         by = c("course_role_id" = "id")) %>% 
              rename(course_role_name = name) %>% 
              select(gamecharacter_id, course_role_name, course_role_id),
            by = "gamecharacter_id") %>% 
  select(-provider) %>% 
  distinct() %>% 
  collect() %>% 
  as.data.table()


# 取得 gamecharacter 於特定 questions 回答的 log ----
# Note: 這邊會使用 BigQuery 是因為 question ID 較多，在一般 MySQL 撈取會很慢
question_ids <- c(unique(series.mission.questions.data$q_id), unique(cross_domain.mission.questions.data$q_id))
options(scipen = 20)    # 將 scipen 設為 20，否則你會在存取 BigQuery 時候遇到以下錯誤
                        # - Invalid value at 'start_index' (TYPE_UINT64), "1e+05" [invalid]
gc.answer.logs.data <- dbGetQuery(conn = conn.list$bqlogsconn,
                                  sprintf("
                                            SELECT id, gamecharacter_id, mission_id, question_id, is_correct, created_at
                                            FROM gc_answer_logs 
                                            WHERE question_id IN (%s)
                                          ", paste0(question_ids, collapse = ",")))


# 結合兩種任務的 mission IDs ----
mission_ids <- c(unique(series.mission.data$id), unique(cross_domain.mission.data$id))


# 取得 gamecharacter 於特定 missions 操作的 log ----
log.gc.mission.actions.tbl <- tbl(conn.list$logsconn, "log_gc_mission_actions")
log.gc.mission.actions.data <- log.gc.mission.actions.tbl %>% 
  filter(mission_id %in% mission_ids) %>% 
  select(mission_id, gamecharacter_id, action_type) %>% 
  collect() %>% 
  as.data.table()


